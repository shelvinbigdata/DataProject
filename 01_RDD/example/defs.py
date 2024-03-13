# coding: utf8
import jieba


def context_jieba(data):
    return list(jieba.cut_for_search(data))


def filter_words(word):
    return word not in ['谷', '帮', '客']


def append_words(word):
    if '传智播' == word:
        word = '传智播客'
    if '院校' == word:
        word = '院校帮'
    if '博学' == word:
        word = '博学谷'
    return word, 1


def extract_id_and_words(id_context):
    user_id = id_context[0]
    context = id_context[1]
    id_word_list = list()
    for word in context_jieba(context):
        if filter_words(word):
            id_word_list.append((user_id + "_" + append_words(word)[0], 1))
    return id_word_list
