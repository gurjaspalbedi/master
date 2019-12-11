# -*- coding: utf-8 -*-

#This function will accept file name and list of lists container words
# ex: [['first_line_word1', 'first_line_word2'], ['second_line_word1', 'second_line_word2']]


def map_func(file_name, input_list):
    word_list = []
    for line in input_list:
        for word in line.split():
            word_list.append((word, '1'))

    return word_list