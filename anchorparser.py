from html.parser import HTMLParser

class AnchorParser(HTMLParser):
    def __init__(self, *, convert_charrefs=True):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr in attrs:
                if attr[0] == 'href':
                    self.hrefs.append(attr[1])
                    break
