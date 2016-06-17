#!/usr/bin/ipython -i

from server import Server, Client
from scottSock import scottSock
import time

from astropy.io import fits
from astro.angles import *
import tempfile


import os
import math

from imgtasks import *
from ds9 import ds9 as DS9

from fits_solver.m4k_imclient import solvefitsfd 
from fits_solver.m4k_imclient import getobjects
from telescope import kuiper
import numpy as np
from threading import Thread
import json

import psutil
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
					
				if not os.path.exists( "{}/reduced_images".format( self.fpath ) ):
					os.mkdir( "{}/reduced_images".format( self.fpath ) )
				if os.path.exists( "{}/reduced_images/r_{}".format( self.fpath, self.fname ) ):
					os.remove( "{}/reduced_images/r_{}".format( self.fpath, self.fname ) )
					 
				reducedImg = fits.open(  "{}/{}".format( self.fpath, self.fname ) )

				#ImageDataList.append( {'img_name': "{}/{}".format(self.fpath, self.fname) } )
				PipeLineTasks.set_file( self.fname, self.fpath )
				PipeLineTasks.set_fitsfd( reducedImg )
				
				tempname = tempfile.mktemp()
				reducedImg.writeto(tempname, clobber=True)
				for task in PipeLineTasks:
					try:

						task_retn = task( reducedImg )
						if type(task_retn) == dict:

							PipeLineTasks.set_tally( task_retn )
							if 'fitsfd' in task_retn.keys():
								if isinstance( task_retn['fitsfd'], fits.hdu.hdulist.HDUList ):
									reducedImg = task_retn['fitsfd']
									
								else:
									raise Exception( "{} did not return fits type, Did you forget to return the fits file in your function definition?".format(task.func_name) )
							
						elif isinstance( task_retn, fits.hdu.hdulist.HDUList ):
								reducedImg = task_retn
						
						
						else:
							raise Exception( "{} did not return fits type, Did you forget to return the fits file in your function definition?".format(task.func_name) )
						
						PipeLineTasks.set_fitsfd( reducedImg )
						reducedImg.writeto(tempname, clobber=True)


						
						print 
					except ZeroDivisionError as err:
						print "there was a problem with task", task.__name__
						print "The message is:", err.message
						print "The args are", 
						for arg in err.args: print arg,
						print
						print 
						print


				print "Tasks Finished"
				print 
				reducedImg.writeto( "{}/reduced_images/r_{}".format( self.fpath, self.fname ) )
				reducedImg.close()
				if os.path.exists(tempname): os.remove(tempname)
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


def leave():
	try:
		myServer.kill()
	except Exception as err:
		print err
	
	exit()


def displayObjects( fitsfd ):
	"""
	Name: displayObjects
Description: 
	Uses python module SEP (https://sep.readthedocs.io/en/v0.5.x/) 
	to extract sources and calulates their FWHM. It assumes the 
	fits file has been merged by the  mergem4k pipeline task. 
	"""
	theDS9 = ds9()
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
		fitsfd[0].header['AVGFWHM'] = avgfwhm
		
	return {'fitsfd':fitsfd, 'avgfwhm':avgfwhm }
	

def WCSsolve( fitsfd ):
	resp = solvefitsfd(fitsfd)

	if 'ra' in resp:

		ra = resp['ra']
	else: 
		ra = None
	if 'dec' in resp:

		dec = resp['dec']
	else:
		dec = resp['dec']	
	
	
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
		focus = tel.reqFOCUS()


		fitsfd[0].header['focus'] = focus
	else:
		focus = None

		
	return {'fitsfd':fitsfd, 'focus':focus }

def showregions( show=False ):
	if show:
		theDS9.set("regions show yes")
	else:
		theDS9.set("regions show no")
	


class _serverThread( Thread ):
	def __init__( self, port=6543, handler=catcher ):
		Thread.__init__(self)
		
		self.server = Server( port, handler)
		
	def run( self ):
		self.server.run()
  
  
  	def kill( self ):
  		self.server.kill()
	

def kill_other(port=6543):
	for pid in psutil.get_pid_list():
		proc = psutil.Process(pid)
		try:
			conns = proc.get_connections()
		except psutil.AccessDenied:
			conns = None
		if conns:
			for x in conns:
				if x.status == psutil.CONN_LISTEN and x.local_address[1] == port:
					print "Uh oh other version of dataserver open!"
					print "Trying to kill process {}".format(pid)
					proc.kill()
						
		
	

def tally( hmm, infos=None ):	
	if infos:
		infolist = infos.split()
	
		
	order = PipeLineTasks.tally_info[-1][1].keys()
	
	for imgname, data in PipeLineTasks.tally_info.iteritems():
		print imgname,
		if infos:
			for info in infolist:
				if info in data:
					print data[info], 
		else:
			for key in order:
				print data[key], 
				
		print
	
def main():
	kill_other()

	global PipeLineTasks
	PipeLineTasks = DataserverPipeLine()

	print hasattr( mergem4k, '__call__' )

	PipeLineTasks[0] = mergem4k
	PipeLineTasks[1] = display
	PipeLineTasks[2] = displayObjects
	PipeLineTasks[3] = getFocus
	PipeLineTasks[4] = WCSsolve



	print "Current PipeLine tasks are:"
	PipeLineTasks.show_tasks()

	print "Starting dataserver thread"
	print "Use leave() to exit so the server shuts down properly."
	global myServer
	myServer = _serverThread( port=6543, handler=catcher )
	myServer.start()


	time.sleep(1.0)
	
	if not myServer.isAlive():
		print "server did not run! Exiting."
		exit()

	ip = get_ipython()
	ip.define_magic("tally", tally)
	ip.define_magic("leave", leave)
	ip.define_magic("exit", leave)
	ip.define_magic("quit", leave)

	
#send_test_image('/home/scott/data/pointing/pointing0004.fits')




main()

def send_images(a):

	send_test_image( '/home/scott/data/pointing/pointing0004.fits', outfile='/home/scott/data/outtest{}.fits'.format(a))



