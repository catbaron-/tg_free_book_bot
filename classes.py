'''
add damean
'''
import queue, datetime
import logging, subprocess
import urllib.request, re, sqlite3
import time, os, telegram, atexit

from threading import Thread
from telegram.ext import Updater
from telegram.ext import CommandHandler
logging.basicConfig(
    filename='fb.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO)

class SubscribedChatID:
    '''
    Assign the chat ids subscribing a type of book    
    '''
    _book_type = ""
    _chat_id = ""
    def __init__(self, book_type, chat_id):
        self._book_type = book_type
        self._chat_id = chat_id

    def get_book_type(self):
        return self._book_type

    def get_chat_id(self):
        return self._chat_id
        
class SubscribedBook:
    '''
    Assign the type of books with keywords and chat ids subscribing this book.
    '''
    def __init__(self, book_type="ANY", book_keyword=""):
        self._book_type = book_type
        self._book_key_word = book_keyword
        self._cids = set()

    def add_chat_id(self, cid):
        self._cids.add(cid)
        return True

    def get_key_word(self):
        return self._book_key_word
        
    def get_book_type(self):
        return self._book_type

    def rm_chat_id(self, cid):
        if cid in self._cids:
            self._cids.remove(cid)
            return True
        return False

    def get_chat_ids(self):
        return self._cids

class FreeBookBot:
    def __init__(self, token=""):
        logging.info("Initializing bot...")
        self._check_url = "https://www.packtpub.com/packt/offers/free-learning"
        # TOKEN for telegram bot
        self._token = token
        self._updater = Updater(token = self._token) 
        # Book of type 'ANY'
        self._subscribed_book_any = SubscribedBook()
        # Books of types other than 'ANY'
        self._subscribed_books = {}
        # Bocation of database
        self._store_path = os.path.abspath(os.path.dirname(__file__))+"/store/"
        self._db_path = self._store_path + "database/database.db"
        # Queues for (un)subscribe
        self._subscribe_queue = queue.Queue()
        self._unsubscribe_queue = queue.Queue()

        # Init subscribed books
        # Generate the books of each type first, then add chat ids to it
        db = sqlite3.connect(self._db_path)
        # generate book object
        for (book_type, book_keyword) in db.execute("select type,"\
        "keyword from booktype where type!='ANY'"):
            self._subscribed_books[book_type] = SubscribedBook(book_type, 
                book_keyword)
        # add chat id to each book
        for (b_type, c_id) in db.execute("select type, chat_id from subscribe"):
            print(b_type+":"+c_id)
            if b_type == "ANY":
                self._subscribed_book_any.add_chat_id(c_id)
            else:
                self._subscribed_books[book_type].add_chat_id(c_id)

    def _db_subscribe(self, q):
        '''
        Database operations for subscribe.
        Read chat_id_obj from queue, and insert the chat id into table `subscribe`
        q: queue of object SubscribeChatID
        '''
        db = sqlite3.connect(self._db_path)
        cur = db.cursor()
        while True:
            time.sleep(3600)
            while not q.empty():
                obj_chat = q.get()
                q_sql = 'insert into subscribe (type, chat_id) values ("{}","{}")'.format(obj_chat.get_book_type(), obj_chat.get_chat_id())
                print(q_sql)
                cur.execute(q_sql)
                logging.info("insert subscribe:%s", q_sql)
                db.commit()
                
    def _db_unsubscribe(self, q):
        '''
        Database operations for unsubscribe.
        Read chat_id_obj from queue, and remove the chat id into table `subscribe`
        q: queue of object SubscribeChatID
        '''
        db = sqlite3.connect(self._db_path)
        cur = db.cursor()
        while True:
            time.sleep(5)
            while not q.empty():
                obj_chat = q.get()
                logging.info("rm record from subscribe: %s", obj_chat.get_chat_id())
                cur.execute('delete from subscribe where chat_id="{}"'.format(obj_chat.get_chat_id()))
                db.commit()

    def _func_start(self, bot, update):
        bot.sendMessage(chat_id=update.message.chat_id,
            text="/checkbook - check today's free book\n"\
            "/books_any - subscribe today's free book\n"\
            "/books_py - subscribe today's free book on Python\n"\
            "/rm_books_any - unsubscribe any of today's free book\n"\
            "/rm_books_py - unsubscribe today's free book on Python\n"\
            "/start, /help - show this message\n")

    def _subscribe(self, cid, book):
        '''
        Inserd the chat_id into subscribe queue, and add it to runtime record.
        cid: chat id
        book: book object of the type the subscribed by the chat_id
        TODO: Add consistency check
        '''
        self._subscribe_queue.put(SubscribedChatID(book.get_book_type(), cid))
        return book.add_chat_id(cid)

    def _unsubscribe(self, cid, book):
        '''
        remove the chat_id into unsubscribe queue, and remove it from runtime record.
        cid: chat id
        book: book object of the type the subscribed by the chat_id
        TODO: Add consistency check
        '''
        self._unsubscribe_queue.put(SubscribedChatID(book.get_book_type(), cid))
        return book.rm_chat_id(cid)

    def _subscribe_func(self, book_type):
        '''
        Factory function generating function to handle subscribe request.
        book_type: type of book subscribed by a chat
        '''
        def func(bot, update):
            cid = update.message.chat_id
            def _t(bot, update):
                if book_type != "ANY":
                    # for book of type other than 'ANY',
                    # unsubscribe it from 'ANY' first, then add it to 
                    # the specific type
                    self._unsubscribe(cid, self._subscribed_book_any)
                    self._subscribe(cid, self._subscribed_books[book_type])
                    newbook = self._checkbook(self._subscribed_books[book_type].get_key_word())
                else:
                    # for book of type 'ANY',
                    # unsubscribe it from other types first, then add it to 
                    # the type of 'ANY'
                    for book in self._subscribed_books.values():
                        self._unsubscribe(cid, book)
                    self._subscribe(cid, self._subscribed_book_any)
                    newbook = self._checkbook(self._subscribed_book_any.get_key_word())
                bot.sendMessage(chat_id=update.message.chat_id,
                    text="You're subscribed from @freebook_today_bot for `{}`".format(book_type))
                if newbook:
                    bot.sendMessage(chat_id=update.message.chat_id,
                        text="Today's free book: `{}`.{}".format(newbook, self._check_url))
            thread = Thread(target=_t, args=(bot, update))
            thread.start()
        return func

    def _unsubscribe_func(self, book_type):
        '''
        Factory function generating function to handle subscribe request.
        book_type: type of book subscribed by a chat
        '''
        def func(bot, update):
            cid = update.message.chat_id
            def _t(bot, update):
                if book_type != "ANY" and self._unsubscribe(cid, self._subscribed_books[book_type]):\
                    msg = "You're unsubscribed from @freebook_today_bot for `{}`".format(book_type)
                else:
                    unsub_types = {"ANY",}
                    self._unsubscribe(cid, self._subscribed_book_any)
                    for book in self._subscribed_book_any.values():
                        self.unsubscribe(cid, book)
                        unsub_types.add(book.get_book_type())
                    msg = "You're not subscribed from @freebook_today_bot for `{}`".format(unsub_types)
                bot.sendMessage(chat_id=update.message.chat_id, text=msg)
            thread = Thread(target=_t, args=(bot, update))
            thread.start()
        return func


    def _checkbook(self, keyword=""):
        url = self._check_url
        # read new book
        html = urllib.request.urlopen(url).read().decode("utf-8")
        txt = re.sub(r"[\t\n]", "", html)
        newbook = re.search(r'dotd-title"><h2>([^>]*)<', txt).group(1)
        print(newbook)
        if not newbook or keyword not in newbook:
            print("No book is found.")
            return
        return newbook

    def _func_checkbook(self, bot, update):
        '''
        Handle request of /checkbook
        For each request create a thread to check the free book and 
        send a message of the result.
        '''
        print("check book request from %s" % update.message.chat_id)
        def _t(bot, update):
            print("check book thread for %s" % update.message.chat_id)
            bot.sendMessage(chat_id=update.message.chat_id,
                text="Checking for you...Pls wait one minute...")
            try:
                newbook = self._checkbook()
                bot.sendMessage(chat_id=update.message.chat_id,
                    text="Taday's free book is `{}`. {}".format(newbook, self._check_url))
            except Exception as e:
                print(e)
        thread = Thread(target=_t, args=(bot, update))
        thread.start()

    def _auto_check(self, bot):
        logging.info("checking book")
        newbook = self._checkbook()
        logging.info("newbook is: %s", newbook)
        dt = datetime.datetime.now()
        if not newbook:
            return
        # read the book name checked last time
        lastbook = ""
        db = sqlite3.connect(self._db_path)
        cur = db.cursor()
        for r in db.execute("select book_name from free_book"):
            lastbook = r[0]
            break
        logging.info("lastbook is: %s", lastbook)
        if lastbook == "":
            logging.warning("lastbook is None")
            logging.info("insert new book %s" % newbook)
            cur.execute("insert into free_book (book_name, check_time) values ('%s', '%s')" % (newbook, dt))
            logging.info("db.commit")
            db.commit()
        print(lastbook)
        # Check if the book is new 
        if ""==lastbook and newbook or newbook and lastbook and newbook.strip() != lastbook.strip():
        # if True:
            # update the new free book
            q_sql = "update free_book set book_name='%s', check_time='%s';" % (newbook, dt)
            logging.info("update free_book: %s ", q_sql)
            cur.execute("""update free_book SET book_name=?, check_time=?""", (newbook, dt))
            logging.info("db.commit")
            db.commit()

            # send message to all user subscribed
            logging.info("sending message...")
            print(self._subscribed_book_any.get_chat_ids())
            for cid in self._subscribed_book_any.get_chat_ids():
                logging.info("send msg to ANY: %s", cid)
                bot.sendMessage(chat_id = cid,
                    text="Today's free book: `{}`. {}".format(newbook, self._check_url))

            print(self._subscribed_books.values())
            for book in self._subscribed_books.values():
                if book.get_key_word() in newbook.lower():
                    for cid in book.get_chat_ids():
                        logging.info("send msg to %s: %s", book.get_book_type(), cid)
                        bot.sendMessage(chat_id = cid,
                            text = "Today's free book ({}) : {}. {}".format(book.get_book_type(), newbook, self._check_url))

    def _loop_auto_check(self):
        bot = telegram.Bot(token=self._token)
        while True:
            self._auto_check(bot)
            time.sleep(3600)
    def run(self):
        dispatcher = self._updater.dispatcher
        dispatcher.add_handler(CommandHandler("start", self._func_start))
        dispatcher.add_handler(CommandHandler("help", self._func_start))
        dispatcher.add_handler(CommandHandler("checkbook", self._func_checkbook))
        dispatcher.add_handler(CommandHandler('books_py', self._subscribe_func("PYTHON")))
        dispatcher.add_handler(CommandHandler('rm_books_py', self._unsubscribe_func("PYTHON")))
        dispatcher.add_handler(CommandHandler('books_any', self._subscribe_func("ANY")))
        dispatcher.add_handler(CommandHandler('rm_books_any', self._unsubscribe_func("ANY")))
        threads = {
            Thread(target=self._loop_auto_check),
            Thread(target=self._db_subscribe, args=(self._subscribe_queue,)),
            Thread(target=self._db_unsubscribe, args=(self._unsubscribe_queue,))
        }
        for t in threads:
            t.start()

        self._updater.start_polling()
        self._updater.idle()
