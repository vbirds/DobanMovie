# -*- coding: utf-8 -*-

import os
import sys
import MySQLdb
import threading

reload(sys)
sys.setdefaultencoding('utf-8')


sql_base = "insert into doubanmovie (m_name, " \
           "m_year, " \
           "m_director, " \
           "m_scriptwriter," \
           "m_classification, " \
           "m_actor," \
           "m_release_date," \
           "m_country," \
           "m_languare," \
           "m_photourl) VALUES (\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\",\"%s\")"

class MovieDao():
    def __init__(self, host, user, passwd, db, file_path):
        self.movies_list = []
        self.thread_num = 10
        self.file_path = file_path
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db

    def text_parse_base(self, line):
        movie_info = []
        movie_info = line.split('\t')
        #print movie_info
        self.movies_list.append(movie_info)

    def import_mysql(self, movie_info, conn):
        #print(movie_info)
        sql = sql_base % (movie_info[1],
                          movie_info[2],
                          movie_info[4],
                          movie_info[5],
                          movie_info[7],
                          movie_info[6],
                          movie_info[10],
                          movie_info[8],
                          movie_info[9],
                          movie_info[3])

        print (sql)
        try:
            curr = conn.cursor()
            curr.execute(sql)
        except Exception,e:
            print("error:", e)
        finally:
            curr.close()

    def get_thread_list(self):
        thread_list = []
        size = len(self.movies_list)
        num = size / self.thread_num
        remainder = size % self.thread_num
        for i in range(self.thread_num):
            thread_list.append([i*num, (i+1)*num])
        thread_list.append([self.thread_num*num, self.thread_num*num + remainder])

        return thread_list


    def batch_import_db(self, movie_list):
        try:
            conn = MySQLdb.connect(host=self.host,
                                        port=3306,
                                        user=self.user,
                                        passwd=self.passwd,
                                        db=self.db,
                                        charset='utf8')
            for movie_index in range(movie_list[0], movie_list[1]):
                self.import_mysql(self.movies_list[movie_index], conn)

            conn.commit()
        except Exception,e:
            print("error:", e)
        finally:
            conn.close()


    def parse_text_and_import_db(self):
        for line in open(self.file_path):
            self.text_parse_base(line)

        thread_list = self.get_thread_list()
        threads = []
        try:
            for list in thread_list:
                t = threading.Thread(target=MovieDao.batch_import_db, args=(self, list))
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
        except Exception,e:
            print("error:", e)
        finally:
            pass

    def start(self):
        self.parse_text_and_import_db()




if __name__=='__main__':
    DoubanMovie = MovieDao('localhost',
                           'root',
                           'admins',
                           'test',
                           '/home/vbirds/PycharmProjects/DoubanMovie/doubanmovie.txt')
    DoubanMovie.start()
