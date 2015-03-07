from pyparsing import *
import urllib2


import pandas as pd
import numpy as np

from sqlalchemy import create_engine,MetaData
from sqlalchemy import Table, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base


#fetch data to work upon
def create_dictionary(data):
	

	#read atom detail which is 5 characters long
	atom = data.read(5)

	dictionary = {}
	dictionary['atom'] = atom

	label = delimitedList(Word(alphas+'-'+nums), delim=' ', combine=True)
	line = Word(nums) + label

	#create dictionary to store observation metadata for atom = 'atom'
	for line_tokens, start_location, end_location in line.scanString(data.readline()):
		dictionary[line_tokens[1]] = line_tokens[0]	

	print dictionary



#Part2
def fill_db(data):


	#begin sqlalchemy

	engine = create_engine('sqlite:///cassis/alchemy/database.db',echo = True)


	meta = MetaData(bind = engine)

	table_datacruncher = Table ('datacruncher',meta,
		Column('id',Integer, primary_key = True),
		Column ('elem',String),
		Column('index',Integer),
		Column('e',Float),
		Column('j',Float),
		Column('label',String),
		Column('gLande',Float)
		)

	colspaces = [(0,9),(10,12),(13,24),(26,29),(30,42),(43,48)]
	dataFrame = pd.read_fwf(data,colspecs=colspaces,skiprows=37,nrows=1764)
	dataFrame.columns=['elem','index','E','J','label','glande']
	conn = engine.raw_connection()
	dataFrame.to_sql(name='datacruncher', con=conn, if_exists='append', index=False)
	conn.close()

if __name__ == '__main__':
	
	data = urllib2.urlopen("http://kurucz.harvard.edu/atoms/1401/gf1401.gam")
	
	print "What would you like to do?"
	print "1) Get Experiment Metadata"
	print "2) Get Observations for the Atom"
	
	option = raw_input()
	

	if option=='1':
		create_dictionary(data)
	elif option=='2':
		fill_db(data)
	else:
		print "invalid option"

	