#!/usr/bin/env python
import requests, os, time

if __name__=="__main__":
	abspath = os.path.abspath(__file__)
	dname = os.path.dirname(abspath)
	os.chdir(dname)

	url = "http://fosm.org/planet/minute-replicate/"
	chunk_size = 1024*1024
	done = False
	sleepTime = 60
	s = requests.Session()
	downloadingOk = True
	cursor = None
	cursorStatus = None 
	
	try:
		cursorFile = open("diffcursor.txt", "rt").read().split("\n")
		i, j, k = map(int, cursorFile[0].split(","))
	except IOError:
		i, j, k = 103, 0, 0

	while not done:
		try:

			while i < 104:
				url1 = "{0}{1:03d}/".format(url,i)
				localpath = "{0}/".format(i)

				if not os.path.exists(localpath):
					resp = s.get(url1)
					if resp.status_code != 200:
						break

				while j < 1000:
					url2 = "{0}{1:03d}/".format(url1,j)
					localpath2 = "{0}{1:03d}/".format(localpath, j)

					if not os.path.exists(localpath2):
						resp = s.get(url2)
						if resp.status_code != 200:
							break

					if not os.path.exists(localpath2):
						os.makedirs(localpath2) #Potential race, if running many instances

					while k < 1000:
						print i, j, k
						singlePairOk = True
						oscUrl = "{0}{1:03d}.osc.gz".format(url2,k)
						oscFina = "{0}{1:03d}.osc.gz".format(localpath2,k)
						if not os.path.exists(oscFina):
							resp = s.get(oscUrl)	
							if resp.status_code == 200:
								fi = open(oscFina, "wb")
								for chunk in resp.iter_content(chunk_size):
									fi.write(chunk)
								fi.close()
							else:
								singlePairOk = False
							if resp.status_code == 404:
								downloadingOk = False

						stateUrl = "{0}{1:03d}.state.txt".format(url2,k)
						stateFina = "{0}{1:03d}.state.txt".format(localpath2,k)
						if not os.path.exists(stateFina):
							resp = s.get(stateUrl)
							statusData = []
							if resp.status_code == 200:
								fi = open(stateFina, "wb")
								for chunk in resp.iter_content(chunk_size):
									statusData.append(chunk)
								fi.write(u"".join(statusData))
								fi.close()
							else:
								singlePairOk = False
							if resp.status_code == 404:
								downloadingOk = False
						else:
							statusData = [open(stateFina, "rb").read()]

						sleepTime = 60
						if not downloadingOk:
							break

						if singlePairOk:
							cursor = (i,j,k)
							cursorStatus = statusData
						k += 1

					if not downloadingOk:
						break

					j += 1
					k = 0

				if not downloadingOk:
					break
				i += 1
				j = 0

			done = True

		except requests.exceptions.ConnectionError as err:
			print err
			sleepTime *= 2
			time.sleep(sleepTime)

	if cursor is not None:
		fi = open("diffcursor.txt", "wt")
		fi.write(",".join(map(str, cursor))+"\n")
		fi.write(u"".join(cursorStatus))
		fi.close()
