from typing import Dict, List, Tuple, Union
import csv

'''
Nice tool to search over CPV's
https://cpvtool.kse.ua/
'''

class DK021:

    LEVEL_RANGE = {
        1: slice(0, 2),
        2: slice(2, 3),
        3: slice(3, 4),
        4: slice(4, 5),
        5: slice(5, 8)
    }

    def __init__(self, description, code=None, parent=None) -> None:
        self.description: str = description
        self.code: str = code
        self.parent: DK021 = parent
        self.children: Dict[str, DK021] = {}

    @classmethod
    def split_code(cls, code:str, sep:str=None)->Union[List[str], str]:
        splitted = [code[sls] for sls in cls.LEVEL_RANGE.values()]
        if sep:
            return sep.join(splitted)
        else:
            return splitted

    def _split_code_description(self, element: str) -> Tuple[str, str]:
        code = element.split(' ')[0].strip()
        assert len(code) == 10
        return code, element[10:].strip()

    def _add_row(self, line: List[str]) -> None:
        top_element = line[0]
        code, description = self._split_code_description(top_element)
        if code not in self.children:
            self.children[code] = DK021(code=code, description=description, parent=self)

        # there are some columns and they are not empty
        if line[1:] and line[1]:
            self.children[code]._add_row(line[1:])

    def get_level_category(self, code, level=3):
        # code consist of 5 levels and control number 11234555-K
        code = code.split('-')[0]
        return self._search_level_category(code=code, level=level)

    def _search_level_category(self, code, level, prefix='', current_level=0):
        current_level +=1
        prefix = prefix + code[self.LEVEL_RANGE[current_level]]

        child = None
        for child_code in self.children.keys():
            if child_code.startswith(prefix):
                child = self.children[child_code]
                break

        if child == None:
            return self # or none/err if no item on such level

        if current_level == level:
            return child
        else:
            return child._search_level_category(
                code=code, 
                level=level, 
                prefix=prefix, 
                current_level=current_level)

    @staticmethod
    def load(path:str): 

        root = DK021(description='root')
        with open(path) as csvfile:
            csvreader = csv.reader(csvfile,)
            next(csvreader) # pass header
            for row in csvreader:
                root._add_row(row)
        
        return root
        


if __name__ == "__main__":
    classifier = DK021.load('./data/dk021.csv')
    print(len(classifier.children))

    norm_cat = classifier.get_level_category('15613300-1') #
    assert norm_cat.code.startswith('15610000')

    print(norm_cat.description)
    