from astropy.io import fits
from matplotlib import pyplot as plt
from scottSock import scottSock
import os
from ds9 import ds9
import collections
from m4kproc import mergem4k
import tempfile
import json

class DataserverPipeLine(object):
	def __init__( self ):
		self.tally_info = TALLY()
		self.tasks = []	
		self.current_objects = None
		self.current_path = None
		self.current_imname = None
		self.current_imdir = None
		#self.current_redname 
		self.current_fitsfd = None
		
	def show_tasks( self, verbose=True ):
		ii=0
		output = '*************************************************************\n'
		for task in self.tasks:
			output+="Task {} {}\n".format( ii, task.__name__)
			if verbose:
				if task.func_doc is not None:
					output+=str(task.func_doc)+'\n'
			ii+=1
		output+="*************************************************************\n"
	
	def add_task(self, func):
		
		if not hasattr( func, '__call__' ):
			raise TypeError("Task must be callable.")
		
		self.tasks.append( func )
	
	def __getitem__( self, ii ):
		return self.tasks[ii]
		
	def __setitem__( self, ii, func ):
		
		if not hasattr( func, '__call__' ):
			raise TypeError("Task must be callable.")
		
		if (ii+1) > len(self.tasks):
			self.tasks.append( func )
		else:
			self.tasks[ii] = func

	def append( self, func ):
		if not hasattr( func, '__call__' ):
			raise TypeError("Task must be callable.")
			
		self.tasks.append( func )


	def pop(self, ii):
		return self.tasks.pop(ii)
	
	def __call__( self ):
		for task in self.tasks:
			print "Attempting task", task.__name__
			try:
				self.current_fitsfd = task( self.current_fitsfd )
			except Exception as err:
				print task.__name__, "has an error"
				print err
			finally:	
				yield self.current_fitsfd
	
	
	def set_tally( self, tallydict ):
		
		self.tally_info[self.current_imname] = tallydict
	
			
	def set_fitsfd( self, fitsfd ):
		self.current_fitsfd = fitsfd
	
	def get_fitsfd( self ):
		return self.current_fitsfd
		
	def __str__(self):
		self.show_tasks()
		
	def __repr__( self ):
		self.show_tasks()

	def set_file(self, fname, fpath):
		self.current_imname = fname
		self.current_imdir = fpath

	def print_tally( self, vals=[] ):
		for key, vals in self.tally_info.iteritems():
			print key
			print json.dumps(vals, indent=4)

	
class TALLY( object ):
	def __init__( self ):
		self.keymap = []
		self.valmap = []
		
	def __setitem__( self, key, val ):

		if 'fitsfd' in val:
			del val['fitsfd']
		

		if (key in self.keymap):
			
			self.valmap[ self.keymap.index(key) ].update(val)

			
		else:
			self.keymap.append( key )
			self.valmap.append( val )
		

			
	def __getitem__( self, key ):
		if type(key) == int:
			return ( self.keymap[key], self.valmap[key] )
			
		else:
			return self.valmap[key]
			
			
	def pop(key):
		if type(key) == int:
			return self.keymap.pop(key), self.valmap.pop(key)
	
	
	def iteritems( self ):
		return zip( self.keymap, self.valmap )
	
	def __str__(self):
		return str(dict(zip(self.keymap, self.valmap)))
				
	def __repr__(self):
		return str(dict(zip(self.keymap, self.valmap)))
def findFocus( imglist ):
	fwhms = []
	focus = []
	
	for imginfo in imglist:		
		if 'avgfwhm' in imginfo.keys() and 'focus' in imginfo.keys():
			fwhms.append( imginfo['avgfwhm'] )
			focus.append( imginfo['focus'] )
			
	plt.plot( focus, fwhms, 'r.' )
	plt.show()
	

def display( fitsfd ):
	myDS9 = ds9()
	fname = fitsfd.filename()
	if fname is None:
		fname = tempfile.mktemp()+".fits"
		fitsfd.writeto(fname)
		fitsfd.close()

	fitsfd = fits.open(fname)
	if len(fitsfd) > 1:

		myDS9.set( "file mosaicimage {}".format(fname) )
		
	elif len(fitsfd) == 1:

		myDS9.set( "file {}".format(fname) )
		
	else:
		raise Exception( "could not display" )


	myDS9.set( "zoom to fit" )
	return fitsfd

def displayMosaic( fitsfd ):
	"""A taks to display the
	mosaic image in dataserver"""
	myDS9 = ds9()
	fname = fitsfd.filename()

	myDS9.set( "file mosaicimage {}".format(fname) )
	myDS9.set("zoom to fit")
	return fitsfd
	
def send_test_image( fname, outfile='/home/scott/data/outtest.fits', clobber=True ):


	fitsfd = fits.open( fname )
	
	width = 0
	height = 0
	for ext in fitsfd:
		if hasattr( ext, 'data' ):
			if ext.data != None:
				width+=ext.data.shape[0]
				height+=ext.data.shape[1]
	
	fitsfd.close()
	fsize = os.stat(fname).st_size
	
	fd = open(fname, 'rb')
	
	
	if clobber:
		clobber_char = '!'
	else:
		clobber_char = ''
	meta = "          {} {}{} 1 {} {} 0".format( fsize, clobber_char, outfile, width, height )
	meta = meta + (256-len(meta))*' '
	
	data = meta+fd.read()
	lendata = len(data)
	soc = scottSock( 'localhost', 6543 )
	
	counter = 0
	socsize = 1024
	buffsize = 0
	while buffsize < len(data):
		sent = soc.send( data[buffsize:buffsize+1024] )
		buffsize+=sent

		
	
	
	
