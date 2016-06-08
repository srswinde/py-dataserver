#!/usr/bin/ipython -i

from server import Server, Client
from scottSock import scottSock
import time

from astropy.io import fits
from astro.angles import *
import tempfile


import os
import math


from ds9 import ds9 as DS9
from m4kproc import mergem4k
from fits_solver.m4k_imclient import solvefitsfd 
from fits_solver.m4k_imclient import getobjects
from telescope import kuiper
import numpy as np
from threading import Thread
import json

try:
	tel=kuiper()
except Exception:
	tel=False

class catcher(Client):
	"""
	This the catcher class that recieves and otherwise 
	deals with incoming images from azcam. It sub-class
	of the Thread class and runs as its own thread. 
	"""
	def __init__( self, (client, address) ):
		Client.__init__( self, (client, address) )
		self.size = 256
		self.count = 0
		self.headerData = False
		self.bytes = 0
		self.inData = False
		self.imgData = []
		self.x = 0
		self.y = 0
		self.dCount = 0
		self.client.settimeout( 0.5 )
		self.clobber = False
		self.ALL = ""
		self.ds9 = theDS9


	def run(self):
		"""
		Most of the important work is done here
		This is called by the Server class and
		starts the new thread. 	
		"""
		running = 1
		while running:
			try:
				data = self.client.recv( self.size )
			except Exception:
				data = None
			
			if data:
				running = self.handle( data )

			else:
				running = 0
				
				
				self.client.close()
				
				if not ( self.fsize == len(self.ALL) ):
					print "Missing data:"
					print "File size should be {}".format(self.fsize)
					print "...but file size is {}".format( len(self.ALL) )

				
				if not os.path.exists( self.fpath ):
					print "Bad path name! {} does not exist.".format(self.fpath)
					return
					
				if not self.clobber and os.path.exists( "{}/{}".format(self.fpath, self.fname) ):
					print "File {}/{} exists and overwrite = NO!.".format(self.fpath, self.fname)
					self.client.close()
					return
				
				if self.clobber and os.path.exists( "{}/{}".format(self.fpath, self.fname) ):
					os.remove( "{}/{}".format(self.fpath, self.fname) )
				
				with open( "{}/{}".format( self.fpath, self.fname ), 'wb' ) as imfile:
					#write the raw image
					if self.ALL.endswith("\n"):
						self.ALL = self.ALL[:-1]
					imfile.write( self.ALL )
				del self.ALL
					
				 
				reducedImg = fits.open(  "{}/{}".format( self.fpath, self.fname ) )
				
				ImageDataList.append( {'img_name': "{}/{}".format(self.fpath, self.fname) } )
				
				for task in PipeLineTasks:
					try:
						print "Attempting task", task.__name__
						reducedImg = task( reducedImg )
						if not isinstance( reducedImg, fits.hdu.hdulist.HDUList ):
							raise Exception( "Task did not return fits type, Did you forget to return the fits file in your function definition?" )
						print task.__name__, "did not give errors."
						
						print 
					except Exception as err:
						print "there was a problem with task", task.__name__
						print "The message is:", err.message
						print "The args are", 
						for arg in err.args: print arg,
						print
						print 
						print
					finally:
						print "******************************************************"
				if not os.path.exists( "{}/reduced_images".format( self.fpath ) ):
					os.mkdir( "{}/reduced_images".format( self.fpath ) )
				if os.path.exists( "{}/reduced_images/{}".format( self.fpath, self.fname ) ):
					os.remove( "{}/reduced_images/{}".format( self.fpath, self.fname ) )
				reducedImg.writeto( "{}/reduced_images/r_{}".format( self.fpath, self.fname ) )
				reducedImg.close()
	
	def handle( self, data ):
		"""
		This method is called in the run method
		when data is availabe. 
		"""
		if self.size == 256:
			#grab the first 256 bytes
			#which is metadata. 
			self.infoStr = data
			
			metavals = self.infoStr.split()


			self.fsize =int( metavals[0] ) 
			self.fpath = os.path.dirname( metavals[1] )
			self.fname = os.path.basename( metavals[1] )
			self.val = int( metavals[2] )
			self.imwidth = int( metavals[3] ) 
			self.imheight = int( metavals[4] )
			self.val2 = int( metavals[4] )
			
			if self.fpath.startswith( '!' ) :
				self.clobber = True
				self.fpath = self.fpath[1:]
			self.size = 1024

		else:
			self.ALL+=data
			

		
		return 1


def display( fitsfd ):
	"""Name: display
	Description: Displays the merged fitsfd file in 
	the current ds9 display. If the MEF has not been merged
	it will not display. 
	"""
	
	if len(fitsfd) == 1:
		theDS9.set_pyfits( fitsfd )
	else:
		print "Does not handle Multiple Extensions yet"
	
	return fitsfd

def displayObjects( fitsfd ):
	"""
		Name: displayObjects
		Description: 
			Uses python module SEP (https://sep.readthedocs.io/en/v0.5.x/) 
			to extract sources and calulates their FWHM. It assumes the 
			fits file has been merged by the  mergem4k pipeline task. 
	"""
	
	objs = getobjects( fitsfd[0].data )
	fwhms = []
	count = 0

	for obj in objs:
		
		fwhm = 2*np.sqrt( math.log(2)*( obj['a'] + obj['b'] ) )

		if 0.1 < obj['a']/obj['b'] and obj['a']/obj['b'] < 10.0:# its fairly round
			if obj['npix'] > 25:# Weed out hot pixels
		

				theDS9.set("regions", 'ellipse( {0} {1} {3} {2} {4} ) '.format( obj['x'], obj['y'], obj['a'], obj['b'], obj['theta']*180/3.141592 -90) )
				theDS9.set('regions', "text {0} {1} # text={{{4:0.2f}}}".format(obj['x'], obj['y']-7, obj['a']/obj['b'], obj['npix'], fwhm ) )
		
				fwhms.append(fwhm)
		
			
		
		count+=1
		if count > 500: break
		
	if len(fwhms) == 0:
		avgfwhm = False
	else:
		avgfwhm = sum(fwhms)/len(fwhms)
	if avgfwhm:
		print "Avg FWHM is ", avgfwhm
		saveImageInfo2List( avgfwhm=avgfwhm )
		fitsfd[0].header['AVGFWHM'] = avgfwhm
		
	return fitsfd
	

def WCSsolve( fitsfd ):
	resp = solvefitsfd(fitsfd)

	if 'ra' in resp:
		print "WCS RA is", resp['ra']
		saveImageInfo2List( ra=resp['ra'] )
	if 'dec' in resp:
		print "WCS Dec is", resp['dec']
		saveImageInfo2List( dec=resp['dec'] )
	
	
	if 'wcs' in resp:
		for key, val in resp['wcs'][0].header.iteritems():
			fitsfd[0].header[key] = val
		fitsfd[0].header[0] = 1
	else: 	
		print 
		print "Image did not solve"
		print "it will be noted in the 'SOLVED' header field"
		fitsfd[0].header["SOLVED"] = 0
	
	return fitsfd

def getFocus(fitsfd):
	if tel:
		focus = tel.reqFOCUS()
		print "focus is ", focus
		saveImageInfo2List( focus=focus )
		fitsfd[0].header['focus'] = focus
	else:
		print "Telescope Communication Err"
	return fitsfd

def showregions( show=False ):
	if show:
		theDS9.set("regions show yes")
	else:
		theDS9.set("regions show no")
	
def saveImageInfo2List( **info ):

	for key, val in info.iteritems():
		ImageDataList[-1][key] = val


	
	
global PipeLineTasks
PipeLineTasks = [ mergem4k, display, displayObjects, getFocus, WCSsolve ]


global ImageDataList
ImageDataList = []

	


	

def leave():
	s.kill()
	exit()	

def testThread(  ):
	while 1:
		print "foo"
		time.sleep(5.0)
		

global theDS9
theDS9 = DS9()



s=Server( port=6543, handler=catcher )
serverThread = Thread( target=s.run )
#serverThread.setDaemon(True)
serverThread.start()










