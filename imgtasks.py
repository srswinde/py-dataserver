from astropy.io import fits
from matplotlib import pyplot as plt


class DataserverPipeLine(object):
	def __init__( self ):
		self.tally_info = {}
		self.tasks = []	
		self.current_objects = None
		self.current_path = None
		self.current_imname = None
		self.current_redname
		self.current_fitsfd
		
	def show_tasks( self, verbose=False ):
		ii=0
		for task in self.tasks:
			print "Task ", ii, task.__name__
			if verbose:
				print task.func_doc
	
	
	def add_task(self, func):
		
		if not hasattr( func, '__call__' ):
			raise TypeError("Task must be callable.")
		
		self.tasks.append( func )
	
	def __getattr__( self, ii ):
		return self.tasks[ii]
		
	def __setattr__( self, ii, func ):
		if not hasattr( func, '__call__' ):
			raise TypeError("Task must be callable.")
		
		self.tasks[ii] = func


	def pop(self, ii):
		return self.tasks.pop(ii)
	
	def __call__( self )
		for task in self.tasks:
			print "Attempting task", task.__name__
			try:
				self.current_fitsfd = task( self.current_fitsfd )
			except Exception as err:
				print task.__name__, "has an error":
				print err
			finally:	
				yield self.current_fitsfd
			
	
	def __str__(self):
		self.show_tasks()
		
	def __repr__( self ):
		self.show_tasks()

def dispayMosaic( fitsfd ) :
	"""A taks to display the
	mosaic image in dataserver"""
	
	

def findFocus( imglist ):
	fwhms = []
	focus = []
	
	for imginfo in imglist:		
		if 'avgfwhm' in imginfo.keys() and 'focus' in imginfo.keys():
			fwhms.append( imginfo['avgfwhm'] )
			focus.append( imginfo['focus'] )
			
	plt.plot( focus, fwhms, 'r.' )
	plt.show()
