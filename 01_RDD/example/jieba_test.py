# coding: utf8
import jieba
if __name__ == '__main__':
    content = "他大舅他二舅都是他舅，树上的鸟儿成双对"
    result1 = jieba.cut(content, True)
    result2 = jieba.cut(content, False)
    result3 = jieba.cut_for_search(content)
    print(list(result1))
    print(list(result2))
    print(list(result3))
    print(type(result3))
