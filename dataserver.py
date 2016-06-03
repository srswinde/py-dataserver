from server import Server, Client
import time

from astropy.io import fits
from astro.angles import *
import tempfile
import shlex
import subprocess
import os
import select
import math
from astro.locales import mtlemmon
import sys
from pyds9 import DS9
from m4kproc import mergem4k
from fits_solver.m4k_imclient import main as imsolver
from fits_solver.m4k_imclient import getobjects, mkfitstable
from telescope import kuiper; tel=kuiper()
import numpy as np
from threading import Thread
from Queue import Empty, Queue; theQueue=Queue()
import json

class catcher(Client):
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
		#self.img = numpy.zeros( ( 765, 510 ), dtype=numpy.int )
		self.clobber = False
		self.ALL = ""
		self.ds9 = theDS9


	def run(self):
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
					imfile.write( self.ALL )
				del self.ALL
					
				
				rawfitsfd = fits.open(  "{}/{}".format( self.fpath, self.fname ) )
				mergedfitsfd = mergem4k( rawfitsfd )
				rawfitsfd.close()
				
				if not os.path.exists( "{}/merged".format( self.fpath  ) ):
					os.mkdir( "{}/merged".format( self.fpath  ) )
				
				mergedfitsfd.writeto( "{}/merged/{}".format( self.fpath, self.fname  ), clobber=self.clobber )
				
				self.ds9.set( "file {}/merged/{}".format( self.fpath, self.fname ) )
				self.ds9.set( "zoom to fit" )
				
				objs = getobjects( mergedfitsfd[0].data )
				fwhms = []
				count = 0
				for obj in objs:
					
					fwhm = 2*np.sqrt(math.log(2)*(obj['a'] + obj['b']))
					
					if 0.1 < obj['a']/obj['b'] and obj['a']/obj['b'] < 10.0:# its fairly round
						if obj['npix'] > 25:# Weed out hot pixels
							
							if showfwhm:
								self.ds9.set("regions", 'ellipse( {0} {1} {3} {2} {4} ) '.format( obj['x'], obj['y'], obj['a'], obj['b'], obj['theta']*180/3.141592 -90) )
								self.ds9.set('regions', "text {0} {1} # text={{{4:0.2f}}}".format(obj['x'], obj['y']-7, obj['a']/obj['b'], obj['npix'], fwhm ) )
							
							fwhms.append(fwhm)
							count+=1
							if count > 500: break
							
				theQueue.put( {"name":self.fname, "fwhm": sum(fwhms)/len(fwhms), 'focus':tel.reqFOCUS()}, block=False )
				"""
				tmpname = tempfile.mktemp()
				fname = "{0}.fits".format( tmpname )
				print "writing fits file", fname
				tmpfd = open( fname, 'wb' )
				tmpfd.write( self.ALL )
				tmpfd.close()
				try:
					self.ds9.set("mosaicimage {0}".format(fname))
					self.ds9.set("scale zscale")
					self.ds9.set("zoom to fit")
				except Exception as err:
					print err
				outstr = self.infoStr.split()[1].replace('!', '')
				
				outpath = os.path.dirname(outstr)
				outfile = os.path.basename(outstr)
				
				outfd = open(outstr, 'wb')
				outfd.write(self.ALL)
				outfd.close()
				os.remove( fname  )
				
				curFocus = int(tel.reqFOCUS())
				extra = {'focus':curFocus }

				tel.comFOCUS(curFocus+10)
				imsolver(outfile, outpath, **extra )
				
				

				print "closing file"
				
				self.client.close()
				"""
				#hdu = fits.PrimaryHDU(numpy.transpose(self.img))
				#hdulist = fits.HDUList([hdu])
				#hdulist.writeto('new.fits')
				#print self.img


	

	def fwhm( fitsfd, extnum=0 ):
		objs = getobjects( fitsfd[extnum].data )
		
		fwhms = []
		
		for obj in objs:
			obj['fwhm'] = 2*np.sqrt(math.log(2)*(obj['a'] + obj['b']))

		return objs
	
	def handle( self, data ):
		if self.size == 256:

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


	
showfwhm = True
def main():
	s=Server( port=6543, handler=catcher )

	s.run()
	


def cmdserver(  ):
	print "Starting Dataserver command server"
	imdata = []
	while 1:
		
		print "dserver>",
		rawin = raw_input().split()
		cmd = rawin[0]
		if len(rawin) > 1:
			args = rawin[1:]
		else:
			args = []
			
		if cmd == "clear":
			theDS9.set( "regions show no" )
		elif cmd == "unclear":
			theDS9.set( "regions show yes" )
		elif cmd == "imdata":
			if args:
				imcount = int(args[0])
			else:
				imcount = 1
			for im in imdata[-imcount:]:
					print json.dumps( im, indent=4 )
			

		while 1:
			if len(imdata) > 100:
					del imdata[0]
			try:
				imdata.append( theQueue.get(block=False) )	
			except Empty:
				break
	
		

global theDS9
theDS9 = DS9()
theDS9.set("regions show no")
SOLVE = False


cmdThread = Thread( target=cmdserver )
cmdThread.setDaemon(True)
cmdThread.start()

main()




