#!/usr/bin/ipython -i

import psutil
from server import Server, Client
from scottSock import scottSock
import time

from astropy.io import fits
from astro.angles import *
import tempfile


import os
import math



from ds9 import ds9


from imgtasks import *

from fits_solver.m4k_imclient import solvefitsfd 

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
						print task.__name__
						task_retn = task( reducedImg )
						reducedImg = task_retn['fitsfd']

						PipeLineTasks.set_tally( task_retn )


						
						reducedImg.writeto(tempname, clobber=True)
						#reducedImg.close()
						#reduceImg = fits.open(tempname)
						PipeLineTasks.set_fitsfd( reducedImg )

						
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


def leave(foo=None):
	try:
		myServer.kill()
	except Exception as err:
		print err
	
	exit()



	

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
		
		self.server = Server( port, handler, tryagain=True )
		
	def run( self ):
		self.server.run()
  
  
  	def kill( self ):
  		self.server.kill()
	

def check_procs(port=6543):
	for proc in psutil.process_iter():
		try:
			conns = proc.connections()
		except psutil.AccessDenied:
			conns = None
		if conns:
			for x in conns:
				if x.status == psutil.CONN_LISTEN and x.laddr[1] == port:
					print "Uh oh other version of dataserver open!"
					print "Trying to kill process {}".format(proc.pid)
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
	#check_procs()

	global PipeLineTasks
	PipeLineTasks = DataserverPipeLine()

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



