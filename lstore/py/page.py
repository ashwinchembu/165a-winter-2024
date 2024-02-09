
class PageSet:

    def __init__(self, num_pages):
        self.pages = [Page() for _ in range(num_pages)]
    

    def has_capacity(self):
        ret = True
        for i in self.pages:
            if i.has_capacity() == False:
                ret = False
        return ret

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = []
    

    def has_capacity(self):
        if (self.num_records * 28) < 4096:
            return True
        else:
            return False

    def write(self, value):
        self.num_records += 1
        self.data.append(value)
        
    def __str__(self):
        return str(self.data)
