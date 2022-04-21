from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import telebot


def get_bot():
    token = ''
    return telebot.TeleBot(token)

i = 0

class FirstTelebot:

    def __init__(self):
        self.bot = get_bot()
        self.chat_id = None
        self.user_id = 0
        self.location = None
        self.user_name = None
        self.description = None
        self.employment_type = None

    def get_kinesis_pub(self):
        return self.kinesis_pub

    def start_events(self):
        i = 0

        @self.bot.message_handler(commands=['start'])
        def process_begin(message):
            self.bot.send_message(chat_id=message.chat.id,
                             text="Do you want to get updates from us?",
                             reply_markup=gen_markup(),
                             parse_mode='HTML')
            self.chat_id = message.chat.id

        def gen_markup():
            markup = InlineKeyboardMarkup()
            markup.row_width = 2
            markup.add(InlineKeyboardButton("Yes", callback_data="cb_yes"),
                       InlineKeyboardButton("No", callback_data="cb_no"))
            return markup

        @self.bot.callback_query_handler(func=lambda call: True)
        def check_saving_user(call):
            global i
            message = call.message.json
            if call.data == "cb_no":
                i = 1 + i
            enter_details(message['chat']['id'])


        def enter_details(message_chat_id):
            global i
            if i == 0:
                self.bot.send_message(message_chat_id, 'Enter ID: ')
                # ID_check(message.text) == True
            elif i == 1:
                self.bot.send_message(message_chat_id, 'Enter description: ')
            elif i == 2:
                self.bot.send_message(message_chat_id, 'Enter location: ')
            elif i == 3:
                self.bot.send_message(message_chat_id, 'Enter employment_type: ')
            else:
                self.bot.stop_polling()
                self.bot.send_message(message_chat_id, 'Searching... ')

        @self.bot.message_handler()
        def save_details(message):
            global i
            if i == 0:
                print(message.text + '  ID')
                check_id = id_check(message.text)
                if check_id == True:
                    self.user_id = message.text
            elif i == 1:
                print(message.text + ' description: ')
                self.description = message.text
            elif i == 2:
                print(message.text + ' location: ')
                self.location = message.text
            elif i == 3:
                print(message.text + ' employment_type: ')
                self.employment_type = message.text
            else:
                print(14)
            i = i + 1
            if i == 1 and check_id == False:
                self.bot.send_message(message.chat.id, 'Wrong ID')
                i = i - 1
            else:
                self.bot.send_message(message.chat.id, 'Thanks')
            enter_details(message.chat.id)


        def id_check(ID_code):
            if (len(ID_code) != 9):
                return False
            try:
                id = list(map(int, ID_code))
            except:
                return False
            counter = 0
            for i in range(9):
                id[i] *= (i % 2) + 1
                if (id[i] > 9):
                    id[i] -= 9
                counter += id[i]
            if (counter % 10 == 0):
                return True
            else:
                return False
