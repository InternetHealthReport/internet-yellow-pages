import sys
import pywikibot
import datetime
from pywikibot.data import api
from pywikibot.exceptions import Error

class UndoBot(object):

    '''Revert changes made in the last one hour.'''

    def __init__(self, lag=60):
        self.lag = lag
        self.site = pywikibot.Site()
        self.user = self.site.username()
        self.start = self.site.server_time()
        self.end = self.site.server_time()-datetime.timedelta(minutes=self.lag)

    def undo_all(self):

        contribs = self.site.usercontribs(user=self.user, start=self.start, end=self.end)
        for item in contribs:
            result = self.revert_one_item(item)
            if result:
                print('{0}: {1}'.format(item['title'], result))
            else:
                print('Skipped {0}'.format(item['title']))

    def revert_one_item(self, item):
        page = pywikibot.Page(self.site, item['title'])

        # Load revision history
        history = list(page.revisions(total=2))
        if len(history) <= 1:
            return False
        rev = history[0]

        try:
        #    self.site.rollbackpage(page, user=self.user, markbot=True)
            self.site.editpage(page, undo=rev['revid'])
        except api.APIError as e:
            if e.code == 'badtoken':
                pywikibot.error(
                    'There was an API token error rollbacking the edit')
                return False
        except Error as e:
            print(e)
            pass
        else:
            return 'The edit(s) made in {} by {} was undoned.'.format(
                page.title(), self.user)

        return False


    def callback(self, item):
        '''Sample callback function for 'private' revert bot.

        @param item: an item from user contributions
        @type item: dict
        @rtype: bool
        '''
        if 'top' in item:
            page = pywikibot.Page(self.site, item['title'])
            text = page.get(get_redirect=True)
            pattern = re.compile(r'\[\[.+?:.+?\..+?\]\]', re.UNICODE)
            return bool(pattern.search(text))
        return False

if __name__ == '__main__':
    if len(sys.argv)<2:
        sys.exit('usage: {sys.argv[0]} last_x_minutes\nUndo changes made in the last X minutes')

    lag = int(sys.argv[1])
    ub = UndoBot(lag)
    ub.undo_all()
