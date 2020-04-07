
def readme(self):
    kind1 = "row['future_hh'] > row[cname] and row['future_hh'] / row['close'] - 1> value"
    kind2 = "((row['high_max'] > row['close'] + row[cname] * plist['art_rate1']) \
                and (row['low_min'] > row['close']) - row[cname] * plist['art_rate2'])"
    print(" kind1: {}\n\n kind2: {}".format(kind1, kind2))

def flag_v1(row, cname1 ='close', cname2='hh'):
    if row[cname1] > row[cname2]:
        return 1
    else:
        return 0

def Y_buy(row, cname='buy_base_price', value=0.001):
    if  row['future_hh'] > row[cname] and row['future_hh'] / row['close'] - 1> value:
        return 1
    else:
        return 0

def Y_sale(row, cname='sale_base_price', value=0.001):
    if  row['future_ll'] < row[cname] and row['future_ll'] / row['close'] -1 < -value:
        return 1
    else:
        return 0
        
def buy_Y_v2(row, plist, cname):

    if ((row['high_max'] > row['close'] + row[cname] * plist['art_rate1'])
            and (row['low_min'] > row['close']) - row[cname] * plist['art_rate2']):
        return 1
    else:
        return 0

def buy_Y_v3(row, plist, cname):
    if (row['current_atr'] <row['future_atr']):
        return 1
    else:
        return 0

def sale_Y(row, num, art_rate3):
    if (row['future_low_' + str(num)] < row['close'] - row['ATR_14']*art_rate3):
        return 1
    else:
        return 0
