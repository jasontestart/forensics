#!/usr/bin/python
 
import sqlite3
import unicodecsv as csv
from sqlite3 import Error
import sys
 
 
def db_connect(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

def get_messages(conn, chatid):
    base_query = 'select m.is_from_me, datetime(j.message_date/1000000000+978307200,"unixepoch","localtime"), datetime(m.date_read/1000000000+978307200,"unixepoch","localtime"),m.text from message m join chat_message_join j on j.message_id = m.rowid where j.chat_id = ? order by j.message_date asc'
    result = []

    cur = conn.cursor()
    cur.execute(base_query,(chatid,))
    messages = cur.fetchall()
    if messages != []:
        result = messages
    return result

def fetch_chats(conn, other_peeps):
    base_query = 'select rowid,chat_identifier,service_name,last_addressed_handle from chat where chat_identifier like ?'
    chats = []
    messages = []
    result = []

    # Find all of the chats that match the parties we are interested in
    for peep in other_peeps:
        cur = conn.cursor()
        this_peep = '%%%s%%' % peep
        cur.execute(base_query,(this_peep,))
        chats += cur.fetchall()

    # Loop through each chat and fetch all of the messages.
    for chat in chats:
        messages = get_messages(conn,chat[0])
        if messages != []:
            for message in messages:
                thisrow = chat + message
                result.append(thisrow)

    return result
 
 
def main():
    usage_msg = 'Usage: %s db-file interlocutors..' % sys.argv[0]
    db_filename = ''
    chat_log = []
    parties = []

    if len(sys.argv) < 3:
        print usage_msg
        return

    db_filename = sys.argv[1]
    for i in range (2, len(sys.argv)):
        parties.append(sys.argv[i])
 
    # create a database connection
    conn = db_connect(db_filename)

    chat_log = fetch_chats(conn,parties)
    if chat_log == []:
        print 'No records found.'
    else:
        with open('chatlog.csv', 'wb') as csvfile:
            chatlogwriter = csv.writer(csvfile, delimiter='\t', quotechar='"', quoting = csv.QUOTE_ALL, encoding='utf-8')
            header_row = ('chatid','other party','service','local_id','direction','date','date_read','message')
            chatlogwriter.writerow(header_row)
            for entry in chat_log:
                this_row = entry[0:4]
                if entry[4] == 1:
                    next_tuple = ('outgoing',entry[5],'n/a',entry[7])
                else:
                    next_tuple = ('incoming',entry[5],entry[6],entry[7])
                this_row += next_tuple
                chatlogwriter.writerow(this_row)
        print 'Success'

if __name__ == '__main__':
    main()
