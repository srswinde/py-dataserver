from astropy.io import fits
from matplotlib import pyplot as plt
from scottSock import scottSock
import os
from pyds9 import ds9
import collections
from m4kproc import mergem4k
import tempfile
import json
from fits_solver.m4k_imclient import getobjects, solvefitsfd
import numpy as np
import math
from telescope import kuiper
import time
try:
	tel=kuiper()
except Exception:
	tel=False


class DataserverPipeLine(object):
	"""Comments !!!"""
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
		return output
	
	def add_task(self, func):
		
		if not hasattr( func, '__call__' ):
			raise TypeError("Task must be callable.")
		
		self.tasks.append( TASK( func ) )
	
	def __getitem__( self, ii ):
		return self.tasks[ii]
		
	def __setitem__( self, ii, func ):
		
		if not hasattr( func, '__call__' ):
			raise TypeError("Task must be callable.")
		
		thisTask = TASK( func )
		
		if (ii+1) > len(self.tasks):
			self.tasks.append( thisTask )
		else:
			self.tasks[ii] = thisTask

	def append( self, func ):
		if not hasattr( func, '__call__' ):
			raise TypeError("Task must be callable.")
			
		self.tasks.append( TASK(func) )


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
		return str( [func.func_name for func in self.tasks] )
		
	def __repr__( self ):
		return self.__str__()

	def set_file(self, fname, fpath):
		self.current_imname = fname
		self.current_imdir = fpath

	def print_tally( self, vals=[] ):
		for key, vals in self.tally_info.iteritems():
			print key
			print json.dumps(vals, indent=4)



class TASK( object ):
	"""Function like object that wraps around
	functions to be added to the DataserverPipelineTasks"""
	def __init__( self, func ):
		self.func = func
		self.__name__ = func.__name__
		self.func_doc = func.func_doc
		self.func_name = func.func_name
		
	def __call__( self, fitsfd ):

		func_resp = self.func( fitsfd )
		dict_resp = {}
		if type(func_resp) == dict:
			dict_resp.update(func_resp)
			if 'fitsfd' not in func_resp.keys():
				dict_resp['fitsfd'] = fitsfd
			else:
				if not isinstance( func_resp['fitsfd'], fits.hdu.hdulist.HDUList ):
					func_resp['fitsfd'] = fitsfd
				
		elif isinstance( func_resp, fits.hdu.hdulist.HDUList ):
			dict_resp['fitsfd'] = func_resp
		
		else:
			dict_resp['fitsfd'] = fitsfd


		return dict_resp
		
	def __str__( self ):
		if self.func.func_doc:
			doc = self.func.func_doc
		else:
			doc = "No Documentation"
			
		return "{}\n\t{}".format( self.func.func_name, doc )
		

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

	
	def __getattr__( self, attr ):
		
		attr_list = []
		for vals in self.valmap:
			for key, value in vals.iteritems():
				if key == attr:
					attr_list.append( value )
					
		return attr_list
			
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


def sextract(fitsfd):
        """
        Name: displayObjects
Description: 
        Uses python module SEP (https://sep.readthedocs.io/en/v0.5.x/) 
        to extract sources and calulates their FWHM. It assumes the 
        fits file has been merged by the  mergem4k pipeline task. 
        """

        objs = getobjects( fitsfd[0].data, 10.0 )
        fwhms = []
        count = 0

        for obj in objs:

                fwhm = 2*np.sqrt( math.log(2)*( obj['a'] + obj['b'] ) )

                if 0.01 < obj['a']/obj['b'] and obj['a']/obj['b'] < 300.0:# its fairly round
                        if obj['npix'] > 25:# Weed out hot pixels



                                fwhms.append(fwhm)



                count+=1
                #if count > 1000: break

        if len(fwhms) == 0:
                avgfwhm = False
        else:
                avgfwhm = sum(fwhms)/len(fwhms)
        if avgfwhm:
                fitsfd[0].header['AVGFWHM'] = avgfwhm

        return {'fitsfd':fitsfd, 'avgfwhm':avgfwhm }
	

def displayObjects( fitsfd ):
	"""
	Name: displayObjects
Description: 
	Uses python module SEP (https://sep.readthedocs.io/en/v0.5.x/) 
	to extract sources and calulates their FWHM. It assumes the 
	fits file has been merged by the  mergem4k pipeline task. 
	"""
	
	theDS9 = ds9()
	if len(fitsfd) == 1:
		objs_list = [getobjects( fitsfd[0].data, 20.0 )]
	else:
		objs_list = [getobjects( fitsfd[1].data, 20.0 ), getobjects( fitsfd[2].data, 20.0 ) ]
	fwhms = []
	count = 0
	for objs in objs_list:
		for obj in objs:
			
			fwhm = 2*np.sqrt( math.log(2)*( obj['a'] + obj['b'] ) )

			if 0.01 < obj['a']/obj['b'] and obj['a']/obj['b'] < 300.0:# its fairly round
				if obj['npix']  > 1:# Weed out hot pixels
		

					theDS9.set("regions", 'ellipse( {0} {1} {3} {2} {4} ) '.format( obj['x']*3, obj['y']*3, obj['a'], obj['b'], obj['theta']*180/3.141592 -90) )
					#theDS9.set('regions', "text {0} {1} # text={{{4:0.2f}}}".format(obj['x'], obj['y']-7, obj['a']/obj['b'], obj['npix'], fwhm ) )
		
					fwhms.append(fwhm)
		
			
		
			count+=1
		#if count > 1000: break
		
	if len(fwhms) == 0:
		avgfwhm = False
	else:
		avgfwhm = sum(fwhms)/len(fwhms)
	if avgfwhm:
		fitsfd[0].header['AVGFWHM'] = avgfwhm
		
	return {'fitsfd':fitsfd, 'avgfwhm':avgfwhm }	

def display( fitsfd ):
	"""
Display an single extension of MEF file in DS9 
	"""

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
	
def send_test_image( fname, outfile='test.fits', clobber=True ):


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
	meta = "          {} {}{} 1 {} {} 0".format( fsize, clobber_char, '/home/scott/'+outfile, width, height )
	meta = meta + (256-len(meta))*' '
	
	data = meta+fd.read()
	lendata = len(data)
	soc = scottSock( 'localhost', 6500 )
	
	counter = 0
	socsize = 1024
	buffsize = 0
	while buffsize < len(data):
		sent = soc.send( data[buffsize:buffsize+1024] )
		buffsize+=sent

		
	
def WCSsolve( fitsfd ):
	resp = solvefitsfd(fitsfd)

	
	
	if 'ra' in resp:
		ra = resp['ra']
	else: 
		ra = None
		
	if 'dec' in resp:
		dec = resp['dec']
	else:
		dec = None
	
	
	if 'wcs' in resp:
		for key, val in resp['wcs'][0].header.iteritems():
			fitsfd[0].header[key] = val
		fitsfd[0].header[0] = 1
	else: 	
		print 
		print "Image did not solve"
		print "it will be noted in the 'SOLVED' header field"
		fitsfd[0].header["SOLVED"] = 0
	
	return {'fitsfd':fitsfd, 'wcsra':ra, 'wcsdec':dec}

def getFocus(fitsfd):
	if tel:
		focus = int(tel.reqFOCUS())

		print "writing focus"
		fitsfd[0].header['focus'] = focus
	else:
		focus = None

		
	return {'fitsfd':fitsfd, 'focus':focus }

def showregions( show=False ):
	if show:
		theDS9.set("regions show yes")
	else:
		theDS9.set("regions show no")	
	



def focusrun(steps=10, stepsize=20):
	steps=int(steps)
	focus=int(tel.reqFOCUS())
	stepsize=int(stepsize)
	s=scottSock("10.30.1.10", 2402)
	for a in range(-steps/2, steps/2):
		tel.comFOCUS(focus+a*stepsize)
		#print PLTasks
		time.sleep(3.0)
		s.converse("exposure.expose1 20.00 'object' 'focusrun'\r\n")
		pixleft = 0
		lpixleft = 0
		while True:
			try:
				pixleft = int( s.converse("exposure.get_pixels_remaining\r\n").strip().split()[-1] )
			except Exception as err:
				print err
			print lpixleft, pixleft
			if lpixleft > 0  and pixleft == 0:
				break
			lpixleft = pixleft
			time.sleep(0.5)
		time.sleep(5.0)
			
		

