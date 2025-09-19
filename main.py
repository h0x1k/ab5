import logging
import os
import html
import sys
import re
import json
import asyncio
from datetime import datetime, timedelta
import pytz
import random

logger = logging.getLogger(__name__)

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

import database
import kb
from parser import SportscheckerParser

# --- Важные настройки ---
try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
except FileNotFoundError:
    print("Ошибка: Файл config.json не найден. Убедитесь, что он существует и находится в той же директории.")
    sys.exit(1)

API_TOKEN = config.get('API_TOKEN')
ADMIN_ID = config.get('ADMIN_ID')
CHANNEL_ID = config.get('CHANNEL_ID')
if CHANNEL_ID:
    try:
        CHANNEL_ID = int(CHANNEL_ID)
    except ValueError:
        logger.error(f"Invalid CHANNEL_ID in config: {CHANNEL_ID}")
        CHANNEL_ID = None

if not API_TOKEN:
    print("Ошибка: API_TOKEN не указан в config.json.")
    sys.exit(1)

# --- ---

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
scheduler = AsyncIOScheduler()
sportschecker_parser = None

# --- Bookmaker Name Mapping ---
def map_bookmaker_name(parser_name):
    """Map parser bookmaker names to database names"""
    mapping = {
        'Betboom': 'Betboom',
        'Fonbet': 'Fonbet',
        'Marathon': 'Marathon',
        'Olimp': 'Olimp',
        'Winline': 'Winline',
    }
    if parser_name in mapping:
        return mapping[parser_name]
    
    clean_name = re.sub(r'\s*\(.*?\)', '', parser_name).strip()
    if clean_name in mapping:
        return mapping[clean_name]
    
    return clean_name

def _filter_and_clean_prediction(prediction_data: dict) -> dict | None:
    logger.info(f"[FILTER TEST] Bypassing filter, accepting all predictions: {prediction_data}")
    return prediction_data

def get_match_key(prediction_data: dict) -> str:
    bookmaker = prediction_data.get('bookmaker', '').strip()
    sport = prediction_data.get('sport', '').strip()
    date = prediction_data.get('date', '').strip()
    teams = prediction_data.get('teams', '').strip()
    date_match = re.match(r'(\d{2}/\d{2})', date)
    date_short = date_match.group(1) if date_match else date
    key = f"{sport}|{date_short}|{teams}"
    return key

def is_admin(user_id):
    user = database.get_user(user_id)
    return user and user['is_admin']

def is_users_db_empty():
    users = database.get_all_users()
    return len(users) == 0

class AdminStates(StatesGroup):
    waiting_for_login = State()
    waiting_for_password = State()
    waiting_for_parsing_interval = State()
    waiting_for_bot_start_time = State()
    waiting_for_bot_end_time = State()
    waiting_for_timezone = State()
    waiting_for_subscription_days = State()
    waiting_for_new_admin_id = State()
    waiting_for_max_signals = State()
    waiting_for_pause_after = State()
    waiting_for_pause_hours = State()
    waiting_for_new_bk_name = State()
    waiting_for_channel_id = State()
    waiting_for_channel_name = State()

class BKManagementStates(StatesGroup):
    managing_user_bks = State()
    managing_channel_bks = State()

# Enhanced parser initialization with better error handling
async def initialize_parser():
    global sportschecker_parser
    login = database.get_setting('sportschecker_login')
    password = database.get_setting('sportschecker_password')
    
    if not login or not password:
        logger.error("❌ Parser credentials not found in database")
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID, "❌ Ошибка: Логин/пароль для парсера не установлены в настройках")
        return False
    
    try:
        if sportschecker_parser:
            sportschecker_parser.close()
        
        # Test parser connection immediately
        test_parser = SportscheckerParser(login, password)
        test_predictions = test_parser.get_predictions()
        test_parser.close()
        
        if test_predictions is None:
            logger.error("❌ Parser failed to get predictions during initialization")
            if ADMIN_ID:
                await bot.send_message(ADMIN_ID, "❌ Ошибка парсера: Не удалось получить прогнозы")
            return False
        
        logger.info(f"✅ Parser initialized successfully. Found {len(test_predictions) if test_predictions else 0} test predictions")
        sportschecker_parser = SportscheckerParser(login, password)
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize parser: {e}")
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID, f"❌ Ошибка инициализации парсера: {str(e)}")
        return False

async def send_admin_panel(chat_id):
    job = scheduler.get_job('send_predictions_job')
    is_parsing_active = job is not None and not job.pending and job.next_run_time is not None
    
    keyboard = kb.admin_panel_keyboard(is_parsing_active)
    await bot.send_message(chat_id, "Добро пожаловать в админ-панель!", reply_markup=keyboard)

async def send_predictions_to_subscribed_users():
    global sportschecker_parser
    
    logger.info("🔍 Starting prediction sending process...")
    
    try:
        # Check parser status
        if sportschecker_parser is None:
            logger.warning("⚠️ Parser not initialized, attempting to initialize...")
            success = await initialize_parser()
            if not success or sportschecker_parser is None:
                logger.error("❌ Failed to initialize parser, skipping this run")
                await schedule_next_run()
                return

        logger.info("🔄 Getting predictions from parser...")
        predictions = sportschecker_parser.get_predictions()
        
        if predictions is None:
            logger.error("❌ Parser returned None instead of predictions list")
            if ADMIN_ID:
                await bot.send_message(ADMIN_ID, "❌ Парсер вернул ошибку (None)")
            await schedule_next_run()
            return
            
        logger.info(f"📊 Parser returned {len(predictions)} predictions")

        if not predictions:
            logger.info("ℹ️ No new predictions found")
            await schedule_next_run()
            return

        new_predictions_to_send = []
        for i, p in enumerate(predictions):
            logger.debug(f"🔍 Processing prediction {i+1}: {p.get('teams', 'Unknown')}")
            
            filtered_p = _filter_and_clean_prediction(p)
            if not filtered_p:
                logger.debug(f"❌ Prediction {i+1} filtered out")
                continue
                
            key = get_match_key(filtered_p)
            if database.is_prediction_sent(key):
                logger.debug(f"⏩ Prediction {i+1} already sent (key: {key})")
                continue
                
            new_predictions_to_send.append((key, filtered_p))
            logger.info(f"✅ New prediction queued: {key}")

        logger.info(f"📨 Ready to send {len(new_predictions_to_send)} new predictions")

        if new_predictions_to_send:
            sent_count = 0
            for key, pred in new_predictions_to_send:
                try:
                    logger.info(f"📤 Sending prediction: {key}")
                    await send_prediction_to_user_and_channel(pred)
                    database.add_sent_prediction(key)
                    sent_count += 1
                    logger.info(f"✅ Successfully sent prediction {sent_count}/{len(new_predictions_to_send)}")
                    
                    # Small delay between sends to avoid rate limiting
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"❌ Failed to send prediction {key}: {e}")
                    continue
                    
            logger.info(f"🎯 Total sent: {sent_count} predictions")
            
        else:
            logger.info("ℹ️ No new predictions to send after filtering")

    except Exception as e:
        logger.error(f"💥 Critical error in prediction sending: {e}", exc_info=True)
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID, f"💥 Критическая ошибка отправки прогнозов: {str(e)}")
    
    finally:
        logger.info("🔄 Scheduling next run...")
        await schedule_next_run()

from aiogram import exceptions

async def send_prediction_to_user_and_channel(prediction_data):
    MONTHS_RU = ['янв.', 'фев.', 'мар.', 'апр.', 'май', 'июнь', 'июль', 'авг.', 'сен.', 'окт.', 'ноя.', 'дек.']
    
    def safe_html(s):
        if s is None: return ""
        s = html.escape(str(s))
        s = re.sub(r'<([^>]+)>', r'&lt;\1&gt;', s)
        return s
    
    # Format date
    date_str = prediction_data.get('date', '').strip()
    formatted_date = date_str
    try:
        dt_obj = datetime.strptime(date_str, '%d/%m %H:%M')
        month_index = dt_obj.month - 1
        formatted_date = f"{dt_obj.day} {MONTHS_RU[month_index]} {dt_obj.strftime('%H:%M')}"
    except (ValueError, IndexError):
        formatted_date = date_str
    
    # Prepare message content
    bookmaker = safe_html(prediction_data.get('bookmaker', ''))
    sport = safe_html(prediction_data.get('sport', ''))
    tournament = safe_html(prediction_data.get('tournament', ''))
    teams = safe_html(prediction_data.get('teams', ''))
    prediction_text = safe_html(prediction_data.get('prediction', ''))
    odd = safe_html(prediction_data.get('odd', ''))
    
    formatted_message = (
        f"<b>BetsLab ЦУПИС V2</b>\n\n"
        f"<b>Букмекерская контора:</b> <i>{bookmaker}</i>\n"
        f"<b>Вид спорта:</b> <i>{sport}</i>\n"
        f"<b>Дата:</b> <i>{safe_html(formatted_date)}</i>\n"
        f"<b>Турнир:</b> <i>{tournament}</i>\n"
        f"<b>Команды:</b> <code>{teams}</code>\n"
        f"<b>Прогноз:</b> <i>{prediction_text}</i>\n"
        f"<b>Коэффициент:</b> <i>{odd}</i>\n"
    )

    bookmaker_name = prediction_data.get('bookmaker', '').strip()
    prediction_key = get_match_key(prediction_data)
    
    logger.info(f"🎯 Processing prediction for distribution: {teams}")
    logger.info(f"📊 Bookmaker: {bookmaker_name}, Key: {prediction_key}")

    if not bookmaker_name:
        logger.warning("⚠️ Prediction missing bookmaker name, skipping")
        return

    # --- Send to Channels ---
    channels = database.get_all_channels()
    logger.info(f"📢 Found {len(channels)} channels in database")
    
    channel_sent = 0
    channel_skipped = 0
    channel_errors = 0

    for channel in channels:
        channel_id = channel['channel_id']
        channel_name = channel['name']
        
        # Skip inactive channels
        if not channel['is_active']:
            logger.debug(f"⏭️ Channel {channel_name} ({channel_id}) is inactive, skipping")
            channel_skipped += 1
            continue
            
        # Check channel bookmaker preferences
        channel_bookmakers = database.get_channel_bookmakers(channel_id)
        channel_bk_names = [bk['name'] for bk in channel_bookmakers if bk['is_selected']]
        
        if channel_bk_names and bookmaker_name not in channel_bk_names:
            logger.debug(f"⏭️ Channel {channel_name} doesn't accept {bookmaker_name}, skipping")
            channel_skipped += 1
            continue
        
        # Send to channel with comprehensive error handling
        try:
            # Verify bot has proper permissions in the channel
            try:
                chat_member = await bot.get_chat_member(channel_id, bot.id)
                if chat_member.status != ChatMemberStatus.ADMINISTRATOR:
                    logger.error(f"❌ Bot is not administrator in channel {channel_name} ({channel_id})")
                    channel_errors += 1
                    continue
                    
                if not chat_member.can_post_messages:
                    logger.error(f"❌ Bot cannot post messages in channel {channel_name} ({channel_id})")
                    channel_errors += 1
                    continue
            except exceptions.ChatNotFound:
                logger.error(f"❌ Channel {channel_name} ({channel_id}) not found")
                database.update_channel(channel_id, is_active=False)
                channel_errors += 1
                continue
            except exceptions.BotKicked:
                logger.error(f"❌ Bot was kicked from channel {channel_name} ({channel_id})")
                database.update_channel(channel_id, is_active=False)
                channel_errors += 1
                continue

            # Send the actual message
            logger.info(f"📤 Sending to channel: {channel_name} ({channel_id})")
            await bot.send_message(channel_id, formatted_message, parse_mode=ParseMode.HTML)
            
            channel_sent += 1
            logger.info(f"✅ Successfully sent to channel: {channel_name} ({channel_id})")
            
        except exceptions.ChatWriteForbidden:
            logger.error(f"❌ Bot cannot write in channel {channel_name} ({channel_id})")
            database.update_channel(channel_id, is_active=False)
            channel_errors += 1
        except exceptions.RetryAfter as e:
            logger.warning(f"⏰ Rate limited for channel {channel_name}, retry after {e.timeout}s")
            await asyncio.sleep(e.timeout)
            # Retry sending after rate limit
            try:
                await bot.send_message(channel_id, formatted_message, parse_mode=ParseMode.HTML)
                channel_sent += 1
                logger.info(f"✅ Successfully sent to channel after rate limit: {channel_name}")
            except Exception as retry_error:
                logger.error(f"❌ Failed retry for channel {channel_name}: {retry_error}")
                channel_errors += 1
        except Exception as e:
            logger.error(f"❌ Failed to send to channel {channel_name} ({channel_id}): {e}")
            channel_errors += 1

    logger.info(f"📊 Channels: {channel_sent} successful, {channel_skipped} skipped, {channel_errors} errors")

    # --- Send to Users ---
    users = database.get_all_active_users()
    logger.info(f"👥 Found {len(users)} active users")
    
    if not users:
        logger.info("ℹ️ No active users to send to")
        await schedule_next_run()
        return

    signal_limits = database.get_signal_limits()
    max_per_day = signal_limits['max_signals_per_day']
    pause_after = signal_limits['pause_after_signals']
    
    user_sent = 0
    user_skipped = 0
    user_errors = 0

    for user in users:
        user_id = user['user_id']
        
        # Check if user is paused
        if database.is_user_paused(user_id):
            logger.debug(f"⏸️ User {user_id} is paused, skipping")
            user_skipped += 1
            continue
            
        # Check daily signal limit
        daily_count = database.get_user_daily_signal_count(user_id)
        if daily_count >= max_per_day:
            logger.debug(f"📊 User {user_id} reached daily limit ({daily_count}/{max_per_day}), skipping")
            user_skipped += 1
            continue
        
        # Check user's bookmaker preferences
        user_bookmakers = database.get_user_bookmakers(user_id)
        if user_bookmakers:
            user_bk_names = [bk['name'] for bk in user_bookmakers]
            if bookmaker_name not in user_bk_names:
                logger.debug(f"🎯 User {user_id} doesn't accept {bookmaker_name}, skipping")
                user_skipped += 1
                continue
        
        # Send to user
        try:
            logger.info(f"📤 Sending to user {user_id}")
            await bot.send_message(user_id, formatted_message, parse_mode=ParseMode.HTML)
            
            database.add_user_prediction(user_id, prediction_key)
            user_sent += 1
            
            new_daily_count = daily_count + 1
            if new_daily_count >= pause_after:
                database.set_user_pause(user_id, signal_limits['pause_duration_hours'])
                logger.info(f"⏸️ User {user_id} paused after {new_daily_count} signals")
                
        except exceptions.BotBlocked:
            logger.error(f"❌ User {user_id} blocked the bot")
            user_errors += 1
        except exceptions.UserDeactivated:
            logger.error(f"❌ User {user_id} deactivated")
            user_errors += 1
        except Exception as e:
            logger.error(f"❌ Failed to send to user {user_id}: {e}")
            user_errors += 1

    logger.info(f"📊 Users: {user_sent} sent, {user_skipped} skipped, {user_errors} errors")

    # --- Final Processing ---
    # Mark prediction as sent only if it was successfully sent to at least one recipient
    if channel_sent > 0 or user_sent > 0:
        database.add_sent_prediction(prediction_key)
        logger.info(f"✅ Prediction {prediction_key} marked as sent")
    else:
        logger.warning(f"⚠️ Prediction {prediction_key} not sent to any recipients")

    await schedule_next_run()

async def schedule_next_run():
    logger.info("⏰ Scheduling next run...")
    
    try:
        if scheduler.get_job('send_predictions_job'):
            scheduler.remove_job('send_predictions_job')
            logger.debug("🗑️ Removed existing job")

        interval_seconds = int(database.get_setting('parsing_interval', 100))
        start_time_str = database.get_setting('working_start_time', '08:00')
        end_time_str = database.get_setting('working_end_time', '23:00')
        tz_str = database.get_setting('timezone', 'Europe/Moscow')

        logger.info(f"⚙️ Settings: interval={interval_seconds}s, time={start_time_str}-{end_time_str}, tz={tz_str}")

        try:
            timezone = pytz.timezone(tz_str)
        except pytz.UnknownTimeZoneError:
            logger.warning(f"⚠️ Unknown timezone {tz_str}, using Europe/Moscow")
            timezone = pytz.timezone('Europe/Moscow')
        
        now = datetime.now(timezone)
        next_run_date = now + timedelta(seconds=interval_seconds + random.randint(0, 30))
        
        scheduler.add_job(
            send_predictions_to_subscribed_users, 
            'date', 
            run_date=next_run_date, 
            timezone=timezone, 
            id='send_predictions_job'
        )
        
        logger.info(f"✅ Next run scheduled for: {next_run_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Send status to admin if it's the first scheduling or after errors
        if ADMIN_ID and (not hasattr(schedule_next_run, 'last_status_sent') or 
                        (now - getattr(schedule_next_run, 'last_status_sent', now - timedelta(hours=1))).total_seconds() > 3600):
            await bot.send_message(ADMIN_ID, f"✅ Планировщик активен\nСледующий запуск: {next_run_date.strftime('%H:%M:%S')}")
            schedule_next_run.last_status_sent = now
            
    except Exception as e:
        logger.error(f"❌ Error in scheduler: {e}")
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID, f"❌ Ошибка планировщика: {str(e)}")
            
async def restart_scheduler():
    scheduler.remove_all_jobs()
    await schedule_next_run()

# --- Basic handlers ---
@dp.message(Command("start"))
async def start_command_handler(message: types.Message):
    user = database.get_user(message.from_user.id)
    if not user:
        if is_users_db_empty():
            database.add_user(message.from_user.id, message.from_user.username, is_admin=True)
            config['ADMIN_ID'] = message.from_user.id
            with open('config.json', 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
            await message.answer("Добро пожаловать! Вы были назначены главным администратором.")
        else:
            database.add_user(message.from_user.id, message.from_user.username)
            await message.answer("Добро пожаловать! Вы были зарегистрированы.")
    
    if is_admin(message.from_user.id):
        await send_admin_panel(message.chat.id)
    else:
        await message.answer("Добро пожаловать в BetsLab VIP bot! Ожидайте прогнозы.")

@dp.message(Command("admin"))
async def admin_command_handler(message: types.Message):
    if is_admin(message.from_user.id):
        await send_admin_panel(message.chat.id)
    else:
        await message.answer("У вас нет прав администратора.")

@dp.message(Command("help"))
async def help_command_handler(message: types.Message):
    await message.answer("Я бот, который отправляет прогнозы со Sportschecker.net. Ожидайте.")

@dp.callback_query(F.data == "back_to_admin_panel")
async def back_to_admin_panel_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    await state.clear()
    await send_admin_panel(callback.from_user.id)
    await callback.answer()

# --- Admin panel handlers ---
@dp.callback_query(F.data == "settings_menu")
async def settings_menu_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    await callback.message.edit_text("Выберите настройку:", reply_markup=kb.settings_menu_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "set_credentials")
async def set_credentials_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    await callback.message.edit_text("Введите логин:")
    await state.set_state(AdminStates.waiting_for_login)
    await callback.answer()

@dp.message(AdminStates.waiting_for_login)
async def process_login(message: types.Message, state: FSMContext):
    await state.update_data(login=message.text)
    await message.answer("Теперь введите пароль:")
    await state.set_state(AdminStates.waiting_for_password)

@dp.message(AdminStates.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    data = await state.get_data()
    login = data.get('login')
    password = message.text
    database.set_setting('sportschecker_login', login)
    database.set_setting('sportschecker_password', password)
    await message.answer("Логин и пароль успешно сохранены.")
    await state.clear()
    await initialize_parser()
    await send_admin_panel(message.chat.id)

@dp.callback_query(F.data == "set_parsing_interval")
async def set_parsing_interval_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    await callback.message.edit_text("Введите интервал парсинга в секундах:")
    await state.set_state(AdminStates.waiting_for_parsing_interval)
    await callback.answer()

@dp.message(AdminStates.waiting_for_parsing_interval)
async def process_parsing_interval(message: types.Message, state: FSMContext):
    try:
        interval_seconds = int(message.text)
        database.set_setting('parsing_interval', interval_seconds)
        await message.answer(f"Интервал парсинга установлен: {interval_seconds} секунд.")
        await state.clear()
        await restart_scheduler()
        await send_admin_panel(message.chat.id)
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число.")
        await state.set_state(AdminStates.waiting_for_parsing_interval)

@dp.callback_query(F.data == "set_working_time")
async def set_working_time_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    await callback.message.edit_text("Введите время начала работы бота (ЧЧ:ММ):")
    await state.set_state(AdminStates.waiting_for_bot_start_time)
    await callback.answer()

@dp.message(AdminStates.waiting_for_bot_start_time)
async def process_start_time(message: types.Message, state: FSMContext):
    if re.match(r'^\d{2}:\d{2}$', message.text):
        await state.update_data(start_time=message.text)
        await message.answer("Теперь введите время окончания работы бота (ЧЧ:ММ):")
        await state.set_state(AdminStates.waiting_for_bot_end_time)
    else:
        await message.answer("Неверный формат. Пожалуйста, введите время в формате ЧЧ:ММ.")
        await state.set_state(AdminStates.waiting_for_bot_start_time)

@dp.message(AdminStates.waiting_for_bot_end_time)
async def process_end_time(message: types.Message, state: FSMContext):
    if re.match(r'^\d{2}:\d{2}$', message.text):
        data = await state.get_data()
        start_time = data.get('start_time')
        database.set_setting('working_start_time', start_time)
        database.set_setting('working_end_time', message.text)
        await message.answer("Время работы бота успешно сохранено.")
        await state.clear()
        await restart_scheduler()
        await send_admin_panel(message.chat.id)
    else:
        await message.answer("Неверный формат. Пожалуйста, введите время в формате ЧЧ:ММ.")
        await state.set_state(AdminStates.waiting_for_bot_end_time)

@dp.callback_query(F.data == "set_timezone")
async def set_timezone_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    await callback.message.edit_text("Введите часовой пояс (например, Europe/Moscow):")
    await state.set_state(AdminStates.waiting_for_timezone)
    await callback.answer()

@dp.message(AdminStates.waiting_for_timezone)
async def process_timezone(message: types.Message, state: FSMContext):
    try:
        pytz.timezone(message.text)
        database.set_setting('timezone', message.text)
        await message.answer(f"Часовой пояс успешно установлен: {message.text}.")
        await state.clear()
        await restart_scheduler()
        await send_admin_panel(message.chat.id)
    except pytz.UnknownTimeZoneError:
        await message.answer("Неверный часовой пояс. Пожалуйста, введите корректный.")
        await state.set_state(AdminStates.waiting_for_timezone)

# --- Subscription management ---
@dp.callback_query(F.data == "subscriptions_menu")
async def subscriptions_menu_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    await callback.message.edit_text("Выберите действие с подписками:", reply_markup=kb.subscriptions_menu_keyboard())
    await callback.answer()

@dp.callback_query(F.data.in_(['add_subscription', 'pause_subscription', 'unpause_subscription', 'cancel_subscription', 'set_admin_user_list', 'user_list_from_subs']))
async def select_user_for_subscription(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    users = database.get_all_users()
    if not users:
        await callback.message.edit_text("Список пользователей пуст.")
        await callback.answer()
        return

    action = callback.data
    keyboard = kb.users_list_keyboard(users, action)
    await callback.message.edit_text("Выберите пользователя:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("add_subscription:"))
async def add_subscription_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    user_id = int(callback.data.split(':')[1])
    await state.update_data(subscription_user_id=user_id)
    await callback.message.edit_text("Введите количество дней подписки:")
    await state.set_state(AdminStates.waiting_for_subscription_days)
    await callback.answer()


@dp.message(AdminStates.waiting_for_subscription_days)
async def process_subscription_days(message: types.Message, state: FSMContext):
    try:
        days = int(message.text)
        if days <= 0:
            await message.answer("Пожалуйста, введите положительное число дней.")
            return
            
        data = await state.get_data()
        user_id = data.get('subscription_user_id')
        
        # Pass days to database function instead of end_date
        database.update_subscription(user_id, days)
        
        # Calculate end date for confirmation message
        end_date = datetime.now() + timedelta(days=days)
        await message.answer(f"✅ Подписка для пользователя {user_id} успешно добавлена на {days} дней.\nОкончание: {end_date.strftime('%Y-%m-%d %H:%M')}")
        await state.clear()
        await send_admin_panel(message.chat.id)
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число дней.")

async def send_with_retry(chat_id, text, parse_mode=None, max_retries=3):
    """Send message with retry logic for network errors"""
    for attempt in range(max_retries):
        try:
            await bot.send_message(chat_id, text, parse_mode=parse_mode)
            return True
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                return False
    return False

@dp.callback_query(F.data.startswith("pause_subscription:"))
async def pause_subscription_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    user_id = int(callback.data.split(':')[1])
    database.pause_subscription(user_id)
    await callback.message.edit_text(f"Подписка пользователя {user_id} успешно приостановлена.")
    await callback.answer()
    await send_admin_panel(callback.from_user.id)

@dp.callback_query(F.data.startswith("unpause_subscription:"))
async def unpause_subscription_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    user_id = int(callback.data.split(':')[1])
    database.unpause_subscription(user_id)
    await callback.message.edit_text(f"Подписка пользователя {user_id} успешно возобновлена.")
    await callback.answer()
    await send_admin_panel(callback.from_user.id)

@dp.callback_query(F.data.startswith("set_admin_user_list:"))
async def set_admin_handler_from_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    user_id = int(callback.data.split(':')[1])
    database.make_admin(user_id)
    await callback.message.edit_text(f"Пользователь с ID {user_id} теперь администратор.")
    await callback.answer()
    await send_admin_panel(callback.from_user.id)

@dp.callback_query(F.data.startswith("cancel_subscription:"))
async def cancel_subscription_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    user_id = int(callback.data.split(':')[1])
    database.cancel_subscription(user_id)
    await callback.message.edit_text(f"Подписка пользователя {user_id} успешно отменена.")
    await callback.answer()
    await send_admin_panel(callback.from_user.id)

@dp.callback_query(F.data.startswith("user_list_from_subs:"))
async def handle_user_selection_from_list(callback: types.CallbackQuery):
    """Обработчик выбора пользователя из списка"""
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    user_id = int(callback.data.split(':')[1])
    user = database.get_user(user_id)
    
    if user:
        # Расчет оставшихся дней подписки
        end_date_str = user.get('end_date')
        days_left = "Нет подписки"
        
        if end_date_str:
            try:
                end_date = datetime.fromisoformat(end_date_str)
                now = datetime.now()
                if end_date > now:
                    days_left_int = (end_date - now).days
                    days_left = f"{days_left_int} дней"
                else:
                    days_left = "Истекла"
            except (ValueError, TypeError):
                days_left = "Ошибка даты"
        
        # Получаем информацию о выбранных БК
        user_bks = database.get_user_bookmakers(user_id)
        bk_text = "все БК ✅" if not user_bks else ", ".join([bk['name'] for bk in user_bks])
        
        # Показываем информацию о пользователе
        user_info = (
            f"👤 Пользователь: @{user['username']}\n"
            f"🆔 ID: {user['user_id']}\n"
            f"👑 Админ: {'Да' if user['is_admin'] else 'Нет'}\n"
            f"📅 Подписка: {days_left}\n"
            f"⏰ Окончание: {end_date_str if end_date_str else 'Н/Д'}\n"
            f"🎯 БК: {bk_text}\n"
            f"⏸️ Статус: {'Активна' if not user.get('is_paused', False) else 'Приостановлена'}"
        )
        
        # Создаем клавиатуру с действиями
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить подписку", callback_data=f"add_subscription:{user_id}")],
            [InlineKeyboardButton(text="⏸️ Приостановить", callback_data=f"pause_subscription:{user_id}")],
            [InlineKeyboardButton(text="▶️ Возобновить", callback_data=f"unpause_subscription:{user_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_subscription:{user_id}")],
            [InlineKeyboardButton(text="📊 Управление БК", callback_data=f"select_user_for_bk:{user_id}")],
            [InlineKeyboardButton(text="👑 Сделать админом", callback_data=f"set_admin_user_list:{user_id}")],
            [InlineKeyboardButton(text="◀️ Назад к списку", callback_data="user_list_from_subs")]
        ])
        
        await callback.message.edit_text(user_info, reply_markup=keyboard)
    else:
        await callback.answer("Пользователь не найден", show_alert=True)
    
    await callback.answer()

# --- Channel management ---
@dp.callback_query(F.data == "channel_settings_menu")
async def channel_settings_menu_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📢 Управление каналами для рассылки:",
        reply_markup=kb.channel_management_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "channel_list")
async def channel_list_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    channels = database.get_all_channels()
    if not channels:
        await callback.message.edit_text("Нет добавленных каналов.", reply_markup=kb.back_to_admin_panel_keyboard())
        await callback.answer()
        return
    
    message = "📢 Список каналов для рассылки:\n\n"
    for channel in channels:
        status = "✅ Активен" if channel['is_active'] else "❌ Неактивен"
        message += f"{channel['name']} (ID: {channel['channel_id']})\nСтатус: {status}\n\n"
    
    await callback.message.edit_text(message, reply_markup=kb.back_to_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "add_channel")
async def add_channel_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    await callback.message.edit_text("Введите ID канала (числовое значение):")
    await state.set_state(AdminStates.waiting_for_channel_id)
    await callback.answer()

@dp.message(AdminStates.waiting_for_channel_id)
async def process_channel_id(message: types.Message, state: FSMContext):
    try:
        channel_id = int(message.text)
        existing_channel = database.get_channel(channel_id)
        if existing_channel:
            await message.answer("Этот канал уже добавлен.")
            await state.clear()
            await send_admin_panel(message.chat.id)
            return
            
        await state.update_data(channel_id=channel_id)
        await message.answer("Теперь введите название канала:")
        await state.set_state(AdminStates.waiting_for_channel_name)
    except ValueError:
        await message.answer("Пожалуйста, введите корректный числовой ID канала:")
        await state.set_state(AdminStates.waiting_for_channel_id)

@dp.message(AdminStates.waiting_for_channel_name)
async def process_channel_name(message: types.Message, state: FSMContext):
    channel_name = message.text.strip()
    if len(channel_name) < 2:
        await message.answer("Название слишком короткое. Попробуйте снова:")
        return
        
    data = await state.get_data()
    channel_id = data.get('channel_id')
    
    database.add_channel(channel_id, channel_name)
    await message.answer(f"Канал '{channel_name}' успешно добавлен!")
    
    await state.clear()
    await send_admin_panel(message.chat.id)

@dp.callback_query(F.data == "manage_channel_bk")
async def manage_channel_bk_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    channels = database.get_all_channels()
    if not channels:
        await callback.message.edit_text("Нет добавленных каналов.", reply_markup=kb.back_to_admin_panel_keyboard())
        await callback.answer()
        return
    
    await callback.message.edit_text(
        "Выберите канал для управления БК:",
        reply_markup=kb.channels_list_keyboard(channels, "select_channel_for_bk")
    )
    await callback.answer()
    
@dp.callback_query(F.data.startswith("select_channel_for_bk:"))
async def select_channel_for_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    channel_id = int(callback.data.split(':')[1])
    channel = database.get_channel(channel_id)
    
    if not channel:
        await callback.answer("Канал не найден.", show_alert=True)
        return
    
    # Get all bookmakers and channel-specific selection status
    all_bookmakers = database.get_all_bookmakers()
    channel_bookmakers = database.get_channel_bookmakers(channel_id)
    
    # Create a mapping of bookmaker IDs to their selection status
    selection_map = {bk['id']: bk['is_selected'] for bk in channel_bookmakers}
    
    # Create bookmaker objects with is_selected property
    bookmakers_with_selection = []
    for bk in all_bookmakers:
        bk_dict = dict(bk) if not isinstance(bk, dict) else bk
        bk_dict['is_selected'] = selection_map.get(bk_dict['id'], False)
        bookmakers_with_selection.append(bk_dict)
    
    selected_ids = [bk['id'] for bk in bookmakers_with_selection if bk['is_selected']]
    
    await state.update_data(
        channel_id=channel_id,
        selected_ids=selected_ids
    )
    await state.set_state(BKManagementStates.managing_channel_bks)
    
    if not selected_ids:
        selected_text = "все БК"
    else:
        selected_names = [bk['name'] for bk in bookmakers_with_selection if bk['is_selected']]
        selected_text = ", ".join(selected_names)
    
    message = (
        f"📊 БК для канала {channel['name']}:\n"
        f"Текущий выбор: {selected_text}\n\n"
        f"Выберите БК для этого канала (можно выбрать несколько):"
    )
    
    keyboard = kb.channel_bookmakers_management_keyboard(bookmakers_with_selection, selected_ids)
    await callback.message.edit_text(message, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(BKManagementStates.managing_channel_bks, F.data.startswith("channel_toggle_bk:"))
async def channel_toggle_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    parts = callback.data.split(':')
    bookmaker_id = int(parts[1])
    
    data = await state.get_data()
    current_selection = data.get('selected_ids', [])
    
    if bookmaker_id in current_selection:
        new_selection = [bk_id for bk_id in current_selection if bk_id != bookmaker_id]
    else:
        new_selection = current_selection + [bookmaker_id]
    
    await state.update_data(selected_ids=new_selection)
    
    # Get all bookmakers and prepare them for the keyboard
    all_bookmakers = database.get_all_bookmakers()
    bookmakers_with_selection = []
    for bk in all_bookmakers:
        bk_dict = dict(bk) if not isinstance(bk, dict) else bk
        bk_dict['is_selected'] = bk_dict['id'] in new_selection
        bookmakers_with_selection.append(bk_dict)
    
    keyboard = kb.channel_bookmakers_management_keyboard(bookmakers_with_selection, new_selection)
    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(BKManagementStates.managing_channel_bks, F.data.startswith("channel_toggle_all_bk:"))
async def channel_toggle_all_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    data = await state.get_data()
    channel_id = data.get('channel_id')
    all_bookmakers = database.get_all_bookmakers()
    
    all_selected = len(data.get('selected_ids', [])) == len([bk for bk in all_bookmakers if bk['is_active']])
    
    if all_selected:
        new_selection = []
    else:
        new_selection = [bk['id'] for bk in all_bookmakers if bk['is_active']]
    
    await state.update_data(selected_ids=new_selection)
    
    # Prepare bookmakers with selection status
    bookmakers_with_selection = []
    for bk in all_bookmakers:
        bk_dict = dict(bk) if not isinstance(bk, dict) else bk
        bk_dict['is_selected'] = bk_dict['id'] in new_selection
        bookmakers_with_selection.append(bk_dict)
    
    keyboard = kb.channel_bookmakers_management_keyboard(bookmakers_with_selection, new_selection)
    await callback.message.edit_reply_markup(reply_markup=keyboard)
    
    action = "отключены" if all_selected else "включены"
    await callback.answer(f"Все БК {action} для канала")

@dp.callback_query(BKManagementStates.managing_channel_bks, F.data == "channel_toggle_all_bk")
async def channel_toggle_all_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    data = await state.get_data()
    bookmakers_for_keyboard = data.get('bookmakers_for_keyboard', [])
    current_selection = data.get('selected_ids', [])
    
    # Get all active bookmaker IDs
    all_active_bookmaker_ids = [bk.get('id') for bk in bookmakers_for_keyboard 
                              if isinstance(bk, dict) and 'id' in bk and bk.get('is_active', True)]
    
    # Check if all active bookmakers are currently selected
    all_selected = all(bk_id in current_selection for bk_id in all_active_bookmaker_ids)
    
    if all_selected:
        # Deselect all - empty selection means "all bookmakers"
        new_selection = []
    else:
        # Select all active bookmakers
        new_selection = all_active_bookmaker_ids.copy()
    
    await state.update_data(selected_ids=new_selection)
    
    # Update the bookmakers_for_keyboard with new selection status
    updated_bookmakers = []
    for bk in bookmakers_for_keyboard:
        if isinstance(bk, dict) and 'id' in bk:
            bk_copy = bk.copy()
            bk_copy['is_selected'] = bk_copy['id'] in new_selection
            updated_bookmakers.append(bk_copy)
        else:
            updated_bookmakers.append(bk)
    
    await state.update_data(bookmakers_for_keyboard=updated_bookmakers)
    
    keyboard = kb.channel_bookmakers_management_keyboard(updated_bookmakers, new_selection)
    await callback.message.edit_reply_markup(reply_markup=keyboard)
    
    action = "отключены" if all_selected else "включены"
    await callback.answer(f"Все БК {action} для канала")

@dp.callback_query(BKManagementStates.managing_channel_bks, F.data == "channel_save_bk")
async def channel_save_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    data = await state.get_data()
    selected_bks = data.get('selected_ids', [])
    channel_id = data.get('channel_id')
    
    # Update all bookmakers for this channel
    bookmakers = database.get_all_bookmakers()
    for bookmaker in bookmakers:
        is_selected = bookmaker['id'] in selected_bks
        database.update_channel_bookmaker(channel_id, bookmaker['id'], is_selected)
    
    channel = database.get_channel(channel_id)
    bookmakers = database.get_all_bookmakers()
    
    if not selected_bks:
        selected_text = "все БК"
    else:
        selected_names = []
        for bk_id in selected_bks:
            bk = next((b for b in bookmakers if b['id'] == bk_id), None)
            if bk:
                selected_names.append(bk['name'])
        selected_text = ", ".join(selected_names)
    
    await callback.message.edit_text(
        f"✅ Выбор БК для канала {channel['name']} сохранен!\n"
        f"Выбранные БК: {selected_text}",
        reply_markup=kb.back_to_admin_panel_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_channel_status:"))
async def toggle_channel_status_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    channel_id = int(callback.data.split(':')[1])
    channel = database.get_channel(channel_id)
    
    if channel:
        new_status = not channel['is_active']
        database.update_channel(channel_id, is_active=new_status)
        
        channels = database.get_all_channels()
        await callback.message.edit_reply_markup(
            reply_markup=kb.channels_list_keyboard(channels, "select_channel_for_bk")
        )
        
        status_text = "активирован" if new_status else "деактивирован"
        await callback.answer(f"Канал {channel['name']} {status_text}")
    else:
        await callback.answer("Канал не найден.", show_alert=True)

@dp.callback_query(F.data.startswith("delete_channel:"))
async def delete_channel_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    channel_id = int(callback.data.split(':')[1])
    channel = database.get_channel(channel_id)
    
    if channel:
        database.delete_channel(channel_id)
        
        channels = database.get_all_channels()
        if channels:
            await callback.message.edit_reply_markup(
                reply_markup=kb.channels_list_keyboard(channels, "select_channel_for_bk")
            )
        else:
            await callback.message.edit_text("Нет добавленных каналов.", reply_markup=kb.back_to_admin_panel_keyboard())
        
        await callback.answer(f"Канал {channel['name']} удален")
    else:
        await callback.answer("Канал не найден.", show_alert=True)

# --- Bookmaker management ---
@dp.callback_query(F.data == "bookmakers_menu")
async def bookmakers_menu_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📊 Управление букмекерскими конторами:",
        reply_markup=kb.bookmakers_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "user_bk_management")
async def user_bk_management_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    users = database.get_all_users()
    if not users:
        await callback.message.edit_text("Список пользователей пуст.")
        await callback.answer()
        return

    keyboard = kb.users_list_keyboard(users, "select_user_for_bk")
    await callback.message.edit_text("Выберите пользователя для управления БК:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_user_for_bk:"))
async def select_user_for_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    user_id = int(callback.data.split(':')[1])
    user = database.get_user(user_id)
    
    if not user:
        await callback.answer("Пользователь не найден.", show_alert=True)
        return
    
    bookmakers = database.get_all_bookmakers()
    user_bookmakers = database.get_user_bookmakers(user_id)
    user_selected_ids = [bk['id'] for bk in user_bookmakers]
    
    await state.update_data(
        user_id=user_id,
        selected_ids=user_selected_ids
    )
    await state.set_state(BKManagementStates.managing_user_bks)
    
    if not user_selected_ids:
        selected_text = "все БК"
    else:
        selected_names = [bk['name'] for bk in user_bookmakers]
        selected_text = ", ".join(selected_names)
    
    message = (
        f"📊 БК для пользователя @{user['username']} (ID: {user_id}):\n"
        f"Текущий выбор: {selected_text}\n\n"
        f"Выберите БК для этого пользователя (можно выбрать несколько):"
    )
    
    keyboard = kb.user_bookmakers_keyboard(user_id, bookmakers, user_selected_ids)
    await callback.message.edit_text(message, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(BKManagementStates.managing_user_bks, F.data.startswith("toggle_bk:"))
async def toggle_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    parts = callback.data.split(':')
    bookmaker_id = int(parts[1])
    
    data = await state.get_data()
    current_selection = data.get('selected_ids', [])
    
    if bookmaker_id in current_selection:
        new_selection = [bk_id for bk_id in current_selection if bk_id != bookmaker_id]
    else:
        new_selection = current_selection + [bookmaker_id]
    
    await state.update_data(selected_ids=new_selection)
    
    bookmakers = database.get_all_bookmakers()
    keyboard = kb.user_bookmakers_keyboard(data['user_id'], bookmakers, new_selection)
    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(BKManagementStates.managing_user_bks, F.data.startswith("toggle_all_bk:"))
async def toggle_all_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    data = await state.get_data()
    user_id = data.get('user_id')
    bookmakers = database.get_all_bookmakers()
    
    all_selected = len(data.get('selected_ids', [])) == len([bk for bk in bookmakers if bk['is_active']])
    
    if all_selected:
        new_selection = []
    else:
        new_selection = [bk['id'] for bk in bookmakers if bk['is_active']]
    
    await state.update_data(selected_ids=new_selection)
    
    keyboard = kb.user_bookmakers_keyboard(user_id, bookmakers, new_selection)
    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(BKManagementStates.managing_user_bks, F.data.startswith("save_bk:"))
async def save_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    data = await state.get_data()
    selected_bks = data.get('selected_ids', [])
    user_id = data.get('user_id')
    
    database.update_user_bookmakers(user_id, selected_bks)
    
    user = database.get_user(user_id)
    bookmakers = database.get_all_bookmakers()
    
    if not selected_bks:
        selected_text = "все БК"
    else:
        selected_names = []
        for bk_id in selected_bks:
            bk = next((b for b in bookmakers if b['id'] == bk_id), None)
            if bk:
                selected_names.append(bk['name'])
        selected_text = ", ".join(selected_names)
    
    await callback.message.edit_text(
        f"✅ Выбор БК для @{user['username']} сохранен!\n"
        f"Выбранные БК: {selected_text}",
        reply_markup=kb.back_to_admin_panel_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "admin_bk_management")
async def admin_bk_management_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    bookmakers = database.get_all_bookmakers()
    message = "📊 Управление букмекерскими конторами:\n\n"
    
    for bookmaker in bookmakers:
        status = "✅ Активна" if bookmaker['is_active'] else "❌ Неактивна"
        message += f"{bookmaker['name']}: {status}\n"
    
    await callback.message.edit_text(message, reply_markup=kb.admin_bookmakers_management_keyboard(bookmakers))
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_toggle_bk:"))
async def admin_toggle_bk_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    bookmaker_id = int(callback.data.split(':')[1])
    bookmakers = database.get_all_bookmakers()
    bookmaker = next((bk for bk in bookmakers if bk['id'] == bookmaker_id), None)
    
    if bookmaker:
        new_status = not bookmaker['is_active']
        database.toggle_bookmaker(bookmaker_id, new_status)
        
        bookmakers = database.get_all_bookmakers()
        await callback.message.edit_reply_markup(
            reply_markup=kb.admin_bookmakers_management_keyboard(bookmakers)
        )
        await callback.answer(f"БК {bookmaker['name']} {'активирована' if new_status else 'деактивирована'}")
    else:
        await callback.answer("БК не найдена.", show_alert=True)

@dp.callback_query(F.data == "add_new_bk")
async def add_new_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    await callback.message.edit_text("Введите название новой букмекерской конторы:")
    await state.set_state(AdminStates.waiting_for_new_bk_name)
    await callback.answer()

@dp.message(AdminStates.waiting_for_new_bk_name)
async def process_new_bk_name(message: types.Message, state: FSMContext):
    bk_name = message.text.strip()
    if len(bk_name) < 2:
        await message.answer("Название слишком короткое. Попробуйте снова:")
        return
    
    database.add_bookmaker(bk_name)
    await message.answer(f"БК '{bk_name}' успешно добавлена!")
    await state.clear()
    await send_admin_panel(message.chat.id)

@dp.callback_query(F.data.startswith("clear_bk:"))
async def clear_bk_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    user_id = int(callback.data.split(':')[1])
    
    await state.update_data(selected_ids=[])
    
    bookmakers = database.get_all_bookmakers()
    keyboard = kb.user_bookmakers_keyboard(user_id, bookmakers, [])
    
    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer("Выбор очищен - пользователь будет получать все БК")

@dp.callback_query(F.data == "set_signal_limits")
async def set_signal_limits_handler(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    current_limits = database.get_signal_limits()
    message = (
        f"Текущие настройки лимитов:\n"
        f"Макс. сигналов в день: {current_limits['max_signals_per_day']}\n"
        f"Пауза после: {current_limits['pause_after_signals']} сигналов\n"
        f"Длительность паузы: {current_limits['pause_duration_hours']} часов\n\n"
        f"Введите максимальное количество сигналов в день:"
    )
    
    await callback.message.edit_text(message)
    await state.set_state(AdminStates.waiting_for_max_signals)
    await callback.answer()

@dp.message(AdminStates.waiting_for_max_signals)
async def process_max_signals(message: types.Message, state: FSMContext):
    try:
        max_signals = int(message.text)
        if max_signals <= 0:
            await message.answer("Число должно быть положительным. Попробуйте снова:")
            return
            
        await state.update_data(max_signals=max_signals)
        await message.answer("Теперь введите количество сигналов, после которого ставить на паузу:")
        await state.set_state(AdminStates.waiting_for_pause_after)
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число:")
        await state.set_state(AdminStates.waiting_for_max_signals)

@dp.message(AdminStates.waiting_for_pause_after)
async def process_pause_after(message: types.Message, state: FSMContext):
    try:
        pause_after = int(message.text)
        data = await state.get_data()
        max_signals = data.get('max_signals')
        
        if pause_after <= 0 or pause_after > max_signals:
            await message.answer(f"Число должно быть от 1 до {max_signals}. Попробуйте снова:")
            return
            
        await state.update_data(pause_after=pause_after)
        await message.answer("Теперь введите длительность паузы в часах:")
        await state.set_state(AdminStates.waiting_for_pause_hours)
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число:")
        await state.set_state(AdminStates.waiting_for_pause_after)

@dp.message(AdminStates.waiting_for_pause_hours)
async def process_pause_hours(message: types.Message, state: FSMContext):
    try:
        pause_hours = int(message.text)
        if pause_hours <= 0:
            await message.answer("Число должно быть положительным. Попробуйте снова:")
            return
            
        data = await state.get_data()
        max_signals = data.get('max_signals')
        pause_after = data.get('pause_after')
        
        database.set_signal_limits(max_signals, pause_after, pause_hours)
        await message.answer(
            f"Лимиты сигналов установлены:\n"
            f"Макс. в день: {max_signals}\n"
            f"Пауза после: {pause_after}\n"
            f"Длительность паузы: {pause_hours} часов"
        )
        await state.clear()
        await send_admin_panel(message.chat.id)
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число:")
        await state.set_state(AdminStates.waiting_for_pause_hours)

# --- Bot status and control ---
@dp.callback_query(F.data == "bot_status")
async def bot_status_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    login = database.get_setting('sportschecker_login')
    password = database.get_setting('sportschecker_password')
    interval = database.get_setting('parsing_interval', 100)
    start_time = database.get_setting('working_start_time', '08:00')
    end_time = database.get_setting('working_end_time', '23:00')
    timezone = database.get_setting('timezone', 'Europe/Moscow')
    total_users = len(database.get_all_users())
    active_users = len(database.get_all_active_users())
    
    signal_limits = database.get_signal_limits()
    
    parser_status = "✅ Инициализирован" if sportschecker_parser else "❌ Не инициализирован"
    
    channels = database.get_all_channels()
    active_channels = [c for c in channels if c['is_active']]
    
    status_message = (
        f"**Статус бота:**\n"
        f"Логин: {login if login else '❌ Не установлен'}\n"
        f"Пароль: {'✅ Установлен' if password else '❌ Не установлен'}\n"
        f"Парсер: {parser_status}\n"
        f"Интервал парсинга: {interval} секунд\n"
        f"Время работы: {start_time} - {end_time}\n"
        f"Часовой пояс: {timezone}\n"
        f"Всего пользователей: {total_users}\n"
        f"Активных пользователей: {active_users}\n"
        f"Каналов: {len(active_channels)}/{len(channels)} активны\n"
        f"Лимиты: {signal_limits['max_signals_per_day']} в день, "
        f"пауза после {signal_limits['pause_after_signals']} сигналов на "
        f"{signal_limits['pause_duration_hours']} часов\n"
    )
    await callback.message.edit_text(status_message, reply_markup=kb.back_to_admin_panel_keyboard(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(F.data == "toggle_parsing")
async def toggle_parsing_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    job = scheduler.get_job('send_predictions_job')
    if job:
        if job.next_run_time is None:
            await schedule_next_run()
            await callback.answer("Парсинг возобновлен!")
        else:
            scheduler.remove_job('send_predictions_job')
            await callback.answer("Парсинг приостановлен!")
    else:
        await callback.answer("Задача парсинга не найдена. Запускаю заново.")
        await schedule_next_run()
    
    await send_admin_panel(callback.from_user.id)

@dp.callback_query(F.data == "debug_info")
async def debug_info_handler(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    
    active_users = database.get_all_active_users()
    total_predictions = database.get_total_predictions_count()
    recent_predictions = database.get_recent_user_predictions(limit=5)
    
    debug_message = (
        f"<b>Информация для отладки:</b>\n\n"
        f"Активных пользователей: {len(active_users)}\n"
        f"Всего отправленных прогнозов: {total_predictions}\n\n"
        f"<b>Последние отправленные прогнозы:</b>\n"
    )
    
    for pred in recent_predictions:
        debug_message += f"Пользователь {pred['user_id']}: {pred['prediction_key']} в {pred['sent_at']}\n"
    
    await callback.message.answer(debug_message, parse_mode=ParseMode.HTML)
    await callback.answer()

@dp.callback_query(F.data == "noop")
async def noop_handler(callback: types.CallbackQuery):
    await callback.answer()

# --- Channel member handler ---
@dp.my_chat_member(F.chat.type == "channel")
async def my_chat_member_handler(my_chat_member: types.ChatMemberUpdated):
    new_member = my_chat_member.new_chat_member
    if new_member.status == ChatMemberStatus.ADMINISTRATOR:
        channel_id = my_chat_member.chat.id
        channel_title = my_chat_member.chat.title
        
        existing_channel = database.get_channel(channel_id)
        if existing_channel:
            database.update_channel(channel_id, is_active=True, name=channel_title)
            logger.info(f"Бот снова добавлен в канал {channel_title} ({channel_id}).")
        else:
            database.add_channel(channel_id, channel_title)
            logger.info(f"Бот добавлен в новый канал {channel_title} ({channel_id}).")
        
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID, f"✅ Бот добавлен в канал {channel_title} ({channel_id})")

# --- Startup and main ---
async def on_startup():
    database.create_tables()
    await initialize_parser()
    
    channels = database.get_all_channels()
    bookmakers = database.get_all_bookmakers()
    
    for channel in channels:
        channel_bookmakers = database.get_channel_bookmakers(channel['channel_id'])
        if not channel_bookmakers:
            for bookmaker in bookmakers:
                database.update_channel_bookmaker(channel['channel_id'], bookmaker['id'], True)
    
    scheduler.add_job(database.delete_old_predictions, 'interval', days=2, id='delete_old_predictions_job')
    scheduler.add_job(database.delete_old_user_predictions, 'interval', days=7, id='delete_old_user_predictions_job')
    scheduler.add_job(database.check_and_resume_users, 'interval', minutes=30, id='check_paused_users_job')
    await restart_scheduler()
    scheduler.start()


async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
        scheduler.shutdown()