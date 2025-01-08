import asyncio
import pandas as pd
import urllib.parse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler,
    CallbackQueryHandler, filters
)
import os
import requests
from datetime import datetime
import logging
from typing import Optional, Dict, Any
import time
import threading
import traceback
import aiohttp
import shlex

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = "TOKEN_BOT"

class UserSettings:
    def __init__(self):
        self.sheet_url: Optional[str] = None
        self.sheet_name: Optional[str] = None
        self.check_interval: int = 60
        self.is_monitoring: bool = False
        self.previous_data: Optional[pd.DataFrame] = None
        self.monitoring_task: Optional[asyncio.Task] = None
        self.notification_threshold: int = 1
        self.last_check_time: Optional[datetime] = None
        self.error_count: int = 0
        self.max_error_count: int = 3
        self.monitored_columns: set = set()  # Добавляем атрибут для отслеживаемых колонок
        self.notification_format: str = 'detailed'  # Добавляем формат уведомлений
        self.last_error_message: Optional[str] = None  # Добавляем хранение последней ошибки
    
def excel_column(index):
    """Преобразует номер столбца в формат Excel (A, B, C, ...)"""
    letters = ''
    index += 1
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters

def get_user_settings(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> UserSettings:
    """Получает настройки пользователя или создает новые."""
    if 'user_settings' not in context.bot_data:
        context.bot_data['user_settings'] = {}
    if user_id not in context.bot_data['user_settings']:
        context.bot_data['user_settings'][user_id] = UserSettings()
    return context.bot_data['user_settings'][user_id]

def create_settings_keyboard() -> InlineKeyboardMarkup:
    """Создание клавиатуры с настройками."""
    keyboard = [
        [
            InlineKeyboardButton("▶️ Начать мониторинг", callback_data='start_monitoring'),
            InlineKeyboardButton("⏹️ Остановить мониторинг", callback_data='stop_monitoring')
        ],
        [InlineKeyboardButton("📈 Статус", callback_data='status')]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start с улучшенным интерфейсом."""
    welcome_message = (
        "👋 Привет! Я бот для мониторинга Google Sheets.\n\n"
        "📋 Основные возможности:\n"
        "1️⃣ Отслеживание изменений в таблицах.\n"
        "2️⃣ Настройка порога уведомлений и фильтров.\n"
        "3️⃣ Подробная статистика изменений.\n\n"
        "💡 Выберите действие из меню команд. Помощь по команде откроется при ее активации без аргументов."
    )
    await update.message.reply_text(welcome_message)

async def set_notification_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Устанавливает порог для отправки уведомлений."""
    user_settings = get_user_settings(context, update.effective_user.id)
    if context.args and context.args[0].isdigit():
        threshold = int(context.args[0])
        if threshold > 0:
            user_settings.notification_threshold = threshold
            await update.message.reply_text(
                f'Порог уведомлений установлен на {threshold} изменений.'
            )
        else:
            await update.message.reply_text('Порог должен быть больше 0.')
    else:
        await update.message.reply_text(
            'Укажите минимальное количество изменений для отправки уведомления.\n'
            'Например: /set_threshold 3'
        )

async def check_sheet(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Улучшенная функция проверки таблицы с человекочитаемыми ячейками."""
    user_settings = get_user_settings(context, user_id)
    
    if not all([user_settings.sheet_url, user_settings.sheet_name]):
        await context.bot.send_message(
            chat_id,
            text='❌ URL таблицы или имя листа не установлены. Используйте /set_sheet для установки.'
        )
        return

    try:
        sheet_id = user_settings.sheet_url.split('/')[5]
        encoded_sheet_name = urllib.parse.quote(user_settings.sheet_name)
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}"

        current_data = pd.read_csv(csv_url, encoding='utf-8', index_col=None)
        user_settings.last_check_time = datetime.now()
        
        # Strip whitespace from column names
        current_data.columns = [col.strip() for col in current_data.columns]
        
        # Rename Unnamed columns to unique names
        for col in current_data.columns:
            if str(col).startswith('Unnamed:'):
                idx = current_data.columns.get_loc(col)
                new_col_name = f'Col_{excel_column(idx)}'
                current_data.rename(columns={col: new_col_name}, inplace=True)
        
        if current_data.empty:
            await context.bot.send_message(
                chat_id,
                text=f'ℹ️ Таблица "{user_settings.sheet_name}" пуста.'
            )
            return

        if user_settings.previous_data is None:
            user_settings.previous_data = current_data.copy()
            await context.bot.send_message(
                chat_id,
                text=f'✅ Начало мониторинга листа "{user_settings.sheet_name}"'
            )
            return
        
        common_cols = set(current_data.columns).intersection(set(user_settings.previous_data.columns))
        added_cols = set(current_data.columns) - set(user_settings.previous_data.columns)
        removed_cols = set(user_settings.previous_data.columns) - set(current_data.columns)

        if user_settings.monitored_columns:
            common_cols = common_cols.intersection(user_settings.monitored_columns)
            added_cols = added_cols.intersection(user_settings.monitored_columns)
            removed_cols = removed_cols.intersection(user_settings.monitored_columns)
        
        changes = []

        # Handle common columns
        for i in range(len(current_data)):
            for col in common_cols:
                old_val = user_settings.previous_data.loc[i, col] if i < len(user_settings.previous_data) else None
                new_val = current_data.loc[i, col]
                if pd.notna(old_val) or pd.notna(new_val):
                    if old_val != new_val:
                        # Получаем позицию столбца в текущем DataFrame
                        col_index = current_data.columns.get_loc(col)
                        cell_addr = f"{excel_column(col_index)}{i+2}"
                        changes.append({
                            'cell': cell_addr,
                            'column': col,
                            'old': old_val,
                            'new': new_val
                        })

        # Handle added columns
        for col in added_cols:
            col_index = current_data.columns.get_loc(col)
            for i in range(len(current_data)):
                new_val = current_data.loc[i, col]
                cell_addr = f"{excel_column(col_index)}{i+2}"
                changes.append({
                    'cell': cell_addr,
                    'column': col,
                    'old': '[НОВАЯ КОЛОНКА]',
                    'new': new_val
                })

        # Handle removed columns
        for col in removed_cols:
            if col in user_settings.previous_data.columns:
                col_index = user_settings.previous_data.columns.get_loc(col)
                for i in range(len(user_settings.previous_data)):
                    old_val = user_settings.previous_data.iloc[i][col]
                    cell_addr = f"{excel_column(col_index)}{i+2}"
                    changes.append({
                        'cell': cell_addr,
                        'column': col,
                        'old': old_val,
                        'new': '[КОЛОНКА УДАЛЕНА]'
                    })

        if len(changes) >= user_settings.notification_threshold:
            if user_settings.notification_format == 'detailed':
                message = "📊 Обнаружены изменения:\n\n"
                for change in changes:
                    message += (f"📍 Ячейка {change['cell']} (колонка '{change['column']}')\n"
                        f"Было: {change['old']}\n"
                        f"Стало: {change['new']}\n\n")
            else:  # compact
                message = "📊 Изменения:\n"
                for change in changes:
                    message += f"📍 {change['cell']}: {change['old']} → {change['new']}\n"
            
            await send_long_message(chat_id, context, message)
            user_settings.error_count = 0
        
        user_settings.previous_data = current_data.copy()

    except Exception as e:
        user_settings.error_count += 1
        error_message = f'❌ Ошибка при чтении таблицы: {str(e)}'
        logger.error(f"Ошибка для пользователя {user_id}: {error_message}")
        
        if user_settings.error_count >= user_settings.max_error_count:
            await context.bot.send_message(
                chat_id,
                text=f"⚠️ Мониторинг остановлен после {user_settings.max_error_count} последовательных ошибок."
            )
            user_settings.is_monitoring = False
            user_settings.monitoring_task = None
        else:
            await context.bot.send_message(chat_id, text=error_message)

async def set_column_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установка фильтра колонок для мониторинга."""
    user_settings = get_user_settings(context, update.effective_user.id)
    if not hasattr(user_settings, 'monitored_columns'):
        user_settings.monitored_columns = set()
    
    if not context.args:
        if user_settings.monitored_columns:
            columns_list = ', '.join(user_settings.monitored_columns)
            await update.message.reply_text(
                f'📊 Текущие отслеживаемые колонки: {columns_list}\n'
                'Для сброса фильтра используйте: /set_column_filter reset'
            )
        else:
            await update.message.reply_text(
                f'📊 Отслеживаются все колонки.\n'
                'Использование: /set_column_filter "колонка 1" "колонка 2" ...\n'
                'Пример: /set_column_filter "Col A" — отслеживает колонку с именем "Col A".\n'
                'Пример: /set_column_filter "Column B, Data" "Col;C" — отслеживает колонки "Column B, Data" и "Col;C".\n\n'
                'Для сброса: /set_column_filter reset\n\n'
				'Внимание! Если в таблице имеются столбцы с объединенными ячейками или они безымянные, используйте следующий формат: Col_A, где A - столбец'
            )
        return

    if context.args[0].lower() == 'reset':
        user_settings.monitored_columns.clear()
        await update.message.reply_text('✅ Фильтр колонок сброшен. Отслеживаются все колонки.')
        return

    args = shlex.split(' '.join(context.args))
    user_settings.monitored_columns = set(args)
    await update.message.reply_text(
        f'✅ Установлен фильтр на колонки: {", ".join(f"'{col}'" for col in user_settings.monitored_columns)}'
    )

async def set_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установка URL и имени листа Google Sheets."""
    user_settings = get_user_settings(context, update.effective_user.id)
    if len(context.args) >= 2:
        user_settings.sheet_url = context.args[0]
        user_settings.sheet_name = ' '.join(context.args[1:])
        user_settings.previous_data = None  # Reset previous data
        if user_settings.monitoring_task:
            user_settings.is_monitoring = False
            user_settings.monitoring_task.cancel()
            user_settings.monitoring_task = None
            await context.bot.send_message(
                update.message.chat_id,
                text='⏹️ Мониторинг остановлен из-за изменения настроек. Чтобы продолжить, начните мониторинг заново с помощью /start_monitoring.'
            )
        await update.message.reply_text(
            f'✅ Настройки обновлены:\n'
            f'📊 URL таблицы: {user_settings.sheet_url}\n'
            f'📑 Имя листа: {user_settings.sheet_name}\n\n'
            f'Предыдущие данные сброшены. Для начала мониторинга используйте /start_monitoring.',
            reply_markup=create_settings_keyboard()
        )
    else:
        await update.message.reply_text(
            '❌ Пожалуйста, укажите URL Google Sheets и имя листа после команды.\n'
            'Пример: /set_sheet https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID Лист1'
        )

async def set_interval(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установка интервала проверки."""
    user_settings = get_user_settings(context, update.effective_user.id)
    if context.args and context.args[0].isdigit():
        interval = int(context.args[0])
        user_settings.check_interval = interval
        
        message = f'⏱️ Интервал проверки установлен на {interval} секунд.'
        if interval < 10:
            message += '\n⚠️ Внимание: маленький интервал может вызвать проблемы в работе.'
            
        await update.message.reply_text(message, reply_markup=create_settings_keyboard())
    else:
        await update.message.reply_text(
            'Используйте команду /set_interval <секунды>\n'
            'Пример: /set_interval 60'
        )

async def send_long_message(chat_id: int, context: ContextTypes.DEFAULT_TYPE, message: str) -> None:
    """Отправка длинных сообщений с разбивкой."""
    max_message_length = 4096
    for i in range(0, len(message), max_message_length):
        chunk = message[i:i + max_message_length]
        await context.bot.send_message(chat_id, text=chunk)

async def start_monitoring(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    """Запуск мониторинга."""
    user_settings = get_user_settings(context, user_id)
    
    if user_settings.monitoring_task is not None:
        user_settings.is_monitoring = False
        await asyncio.sleep(user_settings.check_interval + 1)
        user_settings.monitoring_task.cancel()
        user_settings.monitoring_task = None

    if user_settings.sheet_url and user_settings.sheet_name:
        user_settings.is_monitoring = True
        user_settings.error_count = 0  # Сброс счетчика ошибок
        user_settings.monitoring_task = asyncio.create_task(
            periodic_check(user_id, chat_id, context)
        )
        await context.bot.send_message(chat_id,
            f'✅ Мониторинг запущен\n'
            f'⏱️ Интервал проверки: {user_settings.check_interval} секунд\n'
            f'📊 Порог уведомлений: {user_settings.notification_threshold} изменений',
            reply_markup=create_settings_keyboard()
        )
    else:
        await context.bot.send_message(chat_id,
            '❌ Сначала установите URL таблицы и имя листа с помощью команды /set_sheet'
        )

async def stop_monitoring(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    """Остановка мониторинга."""
    user_settings = get_user_settings(context, user_id)
    user_settings.is_monitoring = False
    
    if user_settings.monitoring_task is not None:
        user_settings.monitoring_task.cancel()
        user_settings.monitoring_task = None
        user_settings.error_count = 0
    
    await context.bot.send_message(chat_id,
        '⏹️ Мониторинг остановлен',
        reply_markup=create_settings_keyboard()
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Проверка текущего статуса мониторинга."""
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        chat_id = query.message.chat_id
    else:
        query = None
        user_id = update.message.from_user.id
        chat_id = update.message.chat_id

    user_settings = get_user_settings(context, user_id)
    last_check = user_settings.last_check_time.strftime("%Y-%m-%d %H:%M:%S") if user_settings.last_check_time else "Нет данных"

	# Форматируем список отслеживаемых колонок
    monitored_cols = "Все колонки" if not user_settings.monitored_columns else ", ".join(f"'{col}'" for col in user_settings.monitored_columns)
    
    # Добавляем информацию о последней ошибке
    last_error = user_settings.last_error_message if hasattr(user_settings, 'last_error_message') and user_settings.last_error_message else "Нет ошибок"
	
    status_message = (
        f"📊 Текущий статус мониторинга:\n\n"
        f"📑 URL таблицы: {user_settings.sheet_url or 'не установлен'}\n"
        f"📋 Имя листа: {user_settings.sheet_name or 'не установлено'}\n"
        f"⏱️ Интервал проверки: {user_settings.check_interval} секунд\n"
        f"🔄 Мониторинг активен: {'да' if user_settings.is_monitoring else 'нет'}\n"
        f"📅 Последняя проверка: {last_check} (GMT+0)\n"
        f"⚠️ Количество ошибок: {user_settings.error_count}/{user_settings.max_error_count}\n"
        f"🎯 Порог уведомлений: {user_settings.notification_threshold} изменений\n"
        f"👁️ Отслеживаемые колонки: {monitored_cols}\n"
        f"📝 Формат уведомлений: {user_settings.notification_format}\n"
        f"❌ Последняя ошибка: {last_error}\n"
    )

    if update.callback_query:
        await query.message.edit_text(status_message, reply_markup=create_settings_keyboard())
    else:
        await context.bot.send_message(chat_id, status_message, reply_markup=create_settings_keyboard())

async def periodic_check(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Периодическая проверка таблицы."""
    user_settings = get_user_settings(context, user_id)
    try:
        while user_settings.is_monitoring:
            try:
                await check_sheet(user_id, chat_id, context)
                await asyncio.sleep(user_settings.check_interval)
            except Exception as e:
                logger.error(f"Ошибка в periodic_check для пользователя {user_id}: {e}")
                await asyncio.sleep(user_settings.check_interval)
    except asyncio.CancelledError:
        logger.info(f"Мониторинг для пользователя {user_id} остановлен")
    finally:
        user_settings.is_monitoring = False
        if user_settings.monitoring_task:
            user_settings.monitoring_task = None

async def root_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик неизвестных команд."""
    await update.message.reply_text(
        "ℹ️ Неизвестная команда. Используйте /start для просмотра доступных команд.",
        reply_markup=create_settings_keyboard()
    )

async def set_notification_format(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установка формата уведомлений."""
    user_settings = get_user_settings(context, update.effective_user.id)
    if not hasattr(user_settings, 'notification_format'):
        user_settings.notification_format = 'detailed'  # или 'compact'
    
    if not context.args:
        await update.message.reply_text(
            'Доступные форматы уведомлений:\n'
            'detailed - подробный формат (по умолчанию)\n'
            'compact - компактный формат\n\n'
            'Использование: /set_notification_format [detailed|compact]'
        )
        return

    format_type = context.args[0].lower()
    if format_type in ['detailed', 'compact']:
        user_settings.notification_format = format_type
        await update.message.reply_text(f'✅ Установлен формат уведомлений: {format_type}')
    else:
        await update.message.reply_text('❌ Неверный формат. Используйте detailed или compact')

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик инлайн-кнопок с исправленной логикой."""
    query = update.callback_query
    await query.answer()

    try:
        if query.data == 'start_monitoring':
            await start_monitoring(update, context, query.message.chat_id, query.from_user.id)
        elif query.data == 'stop_monitoring':
            await stop_monitoring(update, context, query.message.chat_id, query.from_user.id)
        elif query.data == 'status':
            await status(update, context)
       
        elif query.data == 'reset_confirm':
            # Retrieve user settings
            user_settings = get_user_settings(context, query.from_user.id)
            
            # Сброс всех настроек
            user_settings.sheet_url = None
            user_settings.sheet_name = None
            user_settings.check_interval = 60
            user_settings.is_monitoring = False
            user_settings.previous_data = None
            user_settings.notification_threshold = 1
            user_settings.last_check_time = None
            user_settings.error_count = 0
            user_settings.monitored_columns = set()
            user_settings.notification_format = 'detailed'
            
            if user_settings.monitoring_task is not None:
                user_settings.monitoring_task.cancel()
                user_settings.monitoring_task = None
            
            await query.message.edit_text(
                '✅ Настройки и данные успешно сброшены.\n'
                'Вы можете начать настройку бота сначала.'
            )
        elif query.data == 'reset_cancel':
            await query.message.edit_text(
                '❌ Сброс настроек отменен.'
            )
    
    except Exception as e:
        logger.error(f"Ошибка в button_handler: {e}")
        await context.bot.send_message(query.message.chat_id, f"Произошла ошибка: {str(e)}")

async def reset_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сброс всех настроек и данных пользователя."""
    user_settings = get_user_settings(context, update.effective_user.id)
    
    # Отправка сообщения с кнопками для подтверждения сброса
    await update.message.reply_text(
        'Вы уверены, что хотите сбросить все настройки и данные?\n'
        'Это приведет к остановке мониторинга и удалению всех сохраненных параметров.',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data='reset_confirm')],
            [InlineKeyboardButton("❌ Отменить", callback_data='reset_cancel')]
        ])
    )

async def setup_commands(application: Application):
    commands = [
        ("start", "Запустить бота"),
        ("set_sheet", "Установить URL и имя листа"),
        ("set_interval", "Установить интервал проверки"),
        ("set_threshold", "Установить порог изменений"),
        ("start_monitoring", "Запустить мониторинг"),
        ("stop_monitoring", "Остановить мониторинг"),
        ("status", "Проверить статус мониторинга"),
        ("set_column_filter", "Установить фильтр по колонкам"),
        ("set_notification_format", "Установить формат уведомлений"),
        ("reset", "Сбросить все настройки"),
    ]
    await application.bot.set_my_commands(commands)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Централизованный обработчик ошибок."""
    logger.error("Произошла ошибка", exc_info=context.error)
    
    # Если это update от callback query
    if isinstance(update, Update) and update.callback_query:
        try:
            await update.callback_query.answer("Произошла ошибка")
        except:
            pass
    
    # Логирование полной трассировки
    tb_list = traceback.format_exception(
        None, context.error, context.error.__traceback__
    )
    tb_string = "".join(tb_list)
    logger.error(f"Полная трассировка:\n{tb_string}")

def main() -> None:
    """Основная функция запуска бота."""
    application = Application.builder().token(TOKEN).build()

    # Добавляем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("set_sheet", set_sheet))
    application.add_handler(CommandHandler("set_interval", set_interval))
    application.add_handler(CommandHandler("set_threshold", set_notification_threshold))
    application.add_handler(CommandHandler("start_monitoring", lambda update, context: start_monitoring(update, context, update.message.chat_id, update.message.from_user.id)))
    application.add_handler(CommandHandler("stop_monitoring", lambda update, context: stop_monitoring(update, context, update.message.chat_id, update.message.from_user.id)))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("set_column_filter", set_column_filter))
    application.add_handler(CommandHandler("set_notification_format", set_notification_format))
    application.add_handler(CommandHandler("reset", reset_settings))
    application.add_error_handler(error_handler)
    
    # Добавляем обработчик инлайн-кнопок
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Добавляем общий обработчик сообщений
    application.add_handler(MessageHandler(filters.ALL, root_handler))
    
    application.post_init = setup_commands
    
    # Запускаем бота
    application.run_polling()

if __name__ == '__main__':
    main()