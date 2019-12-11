# -*- coding: utf-8 -*-



# Input list will be of format: [[word1, word2....], ['line2_word1, line2_wor2']]

# Output will be of format: [('hello', 'file1.txt'), ('bye', 'file2.txt')]


def map_func(file_name, input_list):
    word_list = []
    for line in input_list:
        for word in line.split():
            word_list.append((word, file_name))

    return word_list