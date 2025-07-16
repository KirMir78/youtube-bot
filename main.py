import os
import logging
import re
import asyncio
import yt_dlp
import tempfile
from aiogram import Bot, Dispatcher, types
from aiogram.types import InputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import ChatMemberStatus
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from datetime import datetime
import contextlib

# Конфигурация
CHAT_VIP = -1001699007474
SUBSCRIPTION_CHANNEL = "https://t.me/showyourmoms"
MAX_FILE_SIZE = 1800 * 1024 * 1024  # 1.8 ГБ в байтах
MAX_CONCURRENT_DOWNLOADS = 3
DOWNLOAD_TIMEOUT = 30 * 60  # 30 минут
MAX_VIDEO_DURATION = 7200  # 2 часа в секундах

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot_errors.log")
    ]
)
logger = logging.getLogger(__name__)

# Загрузка токена
try:
    with open('BotToken.ZB') as TOKENF:
        API_TOKEN = TOKENF.read().strip()
except Exception as e:
    logger.critical(f"Ошибка загрузки токена: {e}")
    exit(1)

# Инициализация бота
storage = MemoryStorage()
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot, storage=storage)

# Семафор для ограничения одновременных загрузок
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Состояния
class DownloadStates(StatesGroup):
    PROCESSING_URL = State()

# Утилиты
def format_duration(seconds: int) -> str:
    """Форматирование длительности видео"""
    if not seconds:
        return "00:00"
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes:02d}:{seconds:02d}"

def format_size(size_bytes: int) -> str:
    """Форматирование размера файла"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"

async def check_subscription(user_id: int) -> bool:
    """Проверка подписки на канал"""
    try:
        chat_member = await bot.get_chat_member(chat_id=CHAT_VIP, user_id=user_id)
        return chat_member.status in [
            ChatMemberStatus.MEMBER, 
            ChatMemberStatus.ADMINISTRATOR, 
            ChatMemberStatus.CREATOR
        ]
    except Exception as e:
        logger.error(f"Ошибка проверки подписки: {e}")
        return False

def get_best_video_format(info: dict) -> dict:
    """Выбор лучшего видеоформата в пределах ограничений"""
    best_format = None
    for f in info.get('formats', []):
        if f.get('filesize') and f['filesize'] <= MAX_FILE_SIZE:
            if f.get('vcodec') != 'none' and f.get('acodec') != 'none':
                # Предпочтение форматам с аудио
                if not best_format or f.get('quality', 0) > best_format.get('quality', 0):
                    best_format = f
    return best_format

def get_best_audio_format(info: dict) -> dict:
    """Выбор лучшего аудиоформата"""
    best_format = None
    for f in info.get('formats', []):
        if f.get('filesize') and f['filesize'] <= MAX_FILE_SIZE:
            if f.get('vcodec') == 'none' and f.get('acodec') != 'none':
                # Выбор формата с наивысшим битрейтом
                if not best_format or f.get('abr', 0) > best_format.get('abr', 0):
                    best_format = f
    return best_format

@contextlib.asynccontextmanager
async def async_timeout(seconds: int):
    """Контекстный менеджер для таймаута операций"""
    try:
        yield
        await asyncio.sleep(0)
    except asyncio.CancelledError:
        raise TimeoutError(f"Operation timed out after {seconds} seconds")

# Обработчики команд
@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    """Обработчик команды /start"""
    keyboard = InlineKeyboardMarkup()
    subscribe_button = InlineKeyboardButton(
        text="👀 Подписаться", 
        url=SUBSCRIPTION_CHANNEL
    )
    keyboard.add(subscribe_button)
    await message.reply(
        "Привет!\n"
        "Я скачиваю видео с YouTube. "
        f"<a href='{SUBSCRIPTION_CHANNEL}'>Подпишись на канал</a> "
        "и отправь мне ссылку на видео.",
        parse_mode="HTML",
        reply_markup=keyboard
    )

@dp.message_handler(regexp=r'^https?://(?:www\.)?(?:youtube\.com|youtu\.be)/.+')
async def handle_youtube_url(message: types.Message, state: FSMContext):
    """Обработчик YouTube ссылок"""
    user = message.from_user
    url = message.text.strip()
    
    # Проверка подписки
    if not await check_subscription(user.id):
        keyboard = InlineKeyboardMarkup()
        subscribe_button = InlineKeyboardButton(
            text="👀 Подписаться", 
            url=SUBSCRIPTION_CHANNEL
        )
        keyboard.add(subscribe_button)
        return await message.reply(
            "❌ Пожалуйста, подпишитесь на канал для использования бота.",
            reply_markup=keyboard
        )
    
    # Проверка на плейлисты и стримы
    if 'list=' in url or 'live' in url:
        return await message.reply("⚠️ Ссылки на плейлисты и стримы не поддерживаются.")
    
    # Сохраняем состояние
    await DownloadStates.PROCESSING_URL.set()
    async with state.proxy() as data:
        data['url'] = url
    
    # Получение информации о видео
    temp_msg = await message.reply("🔍 Получаю информацию о видео...")
    
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'ignoreerrors': True
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
            # Проверка на live-стрим
            if info.get('is_live'):
                return await temp_msg.edit_text("⚠️ Live-стримы не поддерживаются.")
            
            # Проверка длительности
            duration = info.get('duration', 0)
            if duration > MAX_VIDEO_DURATION:
                return await temp_msg.edit_text(f"⚠️ Видео длительностью более {format_duration(MAX_VIDEO_DURATION)} не поддерживаются.")
            
            # Форматирование информации
            title = info.get('title', 'Без названия')[:255]
            views = info.get('view_count', 0)
            upload_date = info.get('upload_date', '')
            channel = info.get('channel', 'Неизвестно')[:100]
            thumbnail_url = info.get('thumbnail', '')
            
            # Форматирование даты
            formatted_date = 'Неизвестно'
            if upload_date and len(upload_date) == 8:  # YYYYMMDD
                try:
                    formatted_date = datetime.strptime(upload_date, '%Y%m%d').strftime('%Y-%m-%d')
                except ValueError:
                    formatted_date = upload_date
            
            caption = (
                f"<b>{title}</b>\n\n"
                f"👁 Просмотров: {format_number_with_spaces(views)}\n"
                f"📅 Дата загрузки: {formatted_date}\n"
                f"👤 Канал: {channel}\n"
                f"⏱ Длительность: {format_duration(duration)}"
            )
            
            # Создание клавиатуры
            keyboard = InlineKeyboardMarkup(row_width=1)
            
            # Получение лучших форматов
            video_format = get_best_video_format(info)
            audio_format = get_best_audio_format(info)
            
            if video_format:
                size_mb = video_format['filesize'] / (1024 * 1024)
                keyboard.add(InlineKeyboardButton(
                    text=f"🎥 Видео ({video_format.get('resolution', 'unknown')}) - {size_mb:.1f}MB",
                    callback_data=f"video:{video_format['format_id']}"
                ))
            
            if audio_format:
                size_mb = audio_format['filesize'] / (1024 * 1024)
                keyboard.add(InlineKeyboardButton(
                    text=f"🔊 Аудио ({audio_format.get('abr', 0)}kbps) - {size_mb:.1f}MB",
                    callback_data=f"audio:{audio_format['format_id']}"
                ))
            
            if not video_format and not audio_format:
                return await temp_msg.edit_text("❌ Не найдено подходящих форматов для скачивания.")
            
            # Отправка результата
            await temp_msg.delete()
            await message.reply_photo(
                photo=thumbnail_url,
                caption=caption,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            
    except yt_dlp.utils.DownloadError as e:
        logger.error(f"Ошибка получения информации: {e}")
        await temp_msg.edit_text("❌ Не удалось получить информацию о видео. Проверьте ссылку.")
    except Exception as e:
        logger.exception("Неизвестная ошибка при обработке URL")
        await temp_msg.edit_text("⚠️ Произошла непредвиденная ошибка. Попробуйте позже.")
    finally:
        await state.finish()

async def download_and_send_media(user_id: int, url: str, format_id: str, media_type: str):
    """Скачивание и отправка медиа без сохранения на диск"""
    with tempfile.TemporaryDirectory() as temp_dir:
        ydl_opts = {
            'format': format_id,
            'outtmpl': os.path.join(temp_dir, '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'socket_timeout': 30,
            'noprogress': True,
            'max_filesize': MAX_FILE_SIZE,
            'merge_output_format': 'mp4' if media_type == 'video' else 'mp3'
        }
        
        try:
            # Скачивание
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                loop = asyncio.get_running_loop()
                info = await loop.run_in_executor(None, lambda: ydl.extract_info(url, download=True))
                
                # Поиск скачанного файла
                file_path = ydl.prepare_filename(info)
                if not os.path.exists(file_path):
                    # Попробуем найти файл по шаблону
                    for f in os.listdir(temp_dir):
                        if f.startswith(info['id']):
                            file_path = os.path.join(temp_dir, f)
                            break
                    else:
                        return "❌ Не удалось найти скачанный файл"
                
                # Проверка размера
                file_size = os.path.getsize(file_path)
                if file_size > MAX_FILE_SIZE:
                    return "❌ Файл слишком большой для отправки"
                
                # Отправка
                with open(file_path, 'rb') as file:
                    if media_type == 'video':
                        await bot.send_video(user_id, InputFile(file))
                    else:
                        await bot.send_audio(user_id, InputFile(file))
                
                return "✅ Файл успешно отправлен"
            
        except yt_dlp.utils.DownloadError as e:
            logger.error(f"Ошибка загрузки: {e}")
            return "❌ Ошибка при загрузке видео"
        except yt_dlp.utils.FileTooLarge:
            return "❌ Файл слишком большой для скачивания"
        except Exception as e:
            logger.exception(f"Ошибка при обработке медиа: {e}")
            return "⚠️ Произошла ошибка при обработке файла"

@dp.callback_query_handler(lambda c: c.data.startswith(('video:', 'audio:')))
async def handle_media_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработчик выбора формата"""
    user = callback_query.from_user
    media_type, format_id = callback_query.data.split(':')
    
    await bot.answer_callback_query(callback_query.id)
    status_msg = await bot.send_message(user.id, "⏳ Начинаю загрузку...")
    
    try:
        async with download_semaphore:
            # Получаем URL из состояния
            data = await state.get_data()
            url = data.get('url')
            
            if not url:
                return await status_msg.edit_text("❌ Сессия истекла, отправьте ссылку снова")
            
            # Создаем задачу с таймаутом
            download_task = asyncio.create_task(
                download_and_send_media(user.id, url, format_id, media_type)
            )
            
            try:
                result = await asyncio.wait_for(download_task, timeout=DOWNLOAD_TIMEOUT)
            except asyncio.TimeoutError:
                download_task.cancel()
                raise TimeoutError("Превышено время загрузки")
                
            await status_msg.edit_text(result)
            
    except TimeoutError:
        await status_msg.edit_text("⌛ Превышено время ожидания загрузки")
    except Exception as e:
        logger.exception(f"Ошибка при обработке запроса: {e}")
        await status_msg.edit_text("⚠️ Произошла критическая ошибка")
    finally:
        await state.finish()

if __name__ == '__main__':
    # Создаем папку для временных файлов, если ее нет
    os.makedirs("downloads", exist_ok=True)
    executor.start_polling(dp, skip_updates=True)
