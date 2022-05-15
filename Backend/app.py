import os
import sys

from flask import Flask, request

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from StockCrawler import Analyzer

app = Flask(__name__)


@app.route('/price')
def price():
    company = request.args.get('company')
    
    start_date = request.args.get('start_date', default = None)
    if start_date is '':
        start_date = None
    
    end_date = request.args.get('end_date', default = None)
    if end_date is '':
        end_date = None
    
    mk = Analyzer.MarketDB()
    
    df = mk.get_daily_price(company, start_date, end_date)
    if type(df) is str:
        return df

    js = df.to_json(orient='index')
    return js


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9090, debug=True)