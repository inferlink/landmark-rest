from ListModel import ListModel


class USCity(ListModel):

    def __init__(self):
        super(USCity, self).__init__()
        self.__city_list = ['new york city', 'los angeles', 'chicago', 'houston', 'philadelphia', 'phoenix',
                            'san antonio', 'san diego', 'dallas', 'san jose', 'austin', 'jacksonville', 'indianapolis',
                            'san francisco', 'columbus', 'fort worth', 'charlotte', 'detroit', 'el paso', 'memphis',
                            'boston', 'seattle', 'denver', 'washington', 'nashville-davidson', 'baltimore',
                            'louisville/jefferson', 'portland', 'oklahoma ', 'milwaukee', 'las vegas', 'albuquerque',
                            'tucson', 'fresno', 'sacramento', 'long beach', 'kansas ', 'mesa', 'virginia beach',
                            'atlanta', 'colorado springs', 'raleigh', 'omaha', 'miami', 'oakland', 'tulsa',
                            'minneapolis', 'cleveland', 'wichita', 'arlington', 'new orleans', 'bakersfield',
                            'tampa', 'honolulu', 'anaheim', 'aurora', 'santa ana', 'st. louis', 'riverside',
                            'corpus christi', 'pittsburgh', 'lexington-fayette', 'anchorage municipality, alaska',
                            'stockton', 'cincinnati', 'st. paul', 'toledo', 'newark', 'greensboro', 'plano',
                            'henderson', 'lincoln', 'buffalo', 'fort wayne', 'jersey ', 'chula vista', 'orlando',
                            'st. petersburg', 'norfolk', 'chandler', 'laredo', 'madison', 'durham', 'lubbock',
                            'winston-salem', 'garland', 'glendale', 'hialeah', 'reno', 'baton rouge',
                            'irvine', 'chesapeake', 'irving', 'scottsdale', 'north las vegas', 'fremont',
                            'gilbert town, arizona', 'san bernardino', 'boise', 'birmingham']

        self.setList(self.__city_list)
        self.setStrict(False)
