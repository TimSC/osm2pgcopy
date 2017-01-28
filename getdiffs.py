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

	while not done:
		try:

			for i in range(103,104):
				url1 = "{0}{1:03d}/".format(url,i)
				localpath = "{0}/".format(i)

				if not os.path.exists(localpath):
					resp = s.get(url1)
					if resp.status_code != 200:
						break

				for j in range(0,1000):
					url2 = "{0}{1:03d}/".format(url1,j)
					localpath2 = "{0}{1:03d}/".format(localpath, j)

					if not os.path.exists(localpath2):
						resp = s.get(url2)
						if resp.status_code != 200:
							break

					if not os.path.exists(localpath2):
						os.makedirs(localpath2) #Potential race, if running many instances

					oscFina = "{0}{1:03d}.osc.gz".format(localpath2,999)
					stateFina = "{0}{1:03d}.state.txt".format(localpath2,999)
					if os.path.exists(oscFina) and os.path.exists(stateFina):
						continue #This folder is probably complete because last files exist

					for k in range(1000):
						print i, j, k
						oscUrl = "{0}{1:03d}.osc.gz".format(url2,k)
						oscFina = "{0}{1:03d}.osc.gz".format(localpath2,k)
						if not os.path.exists(oscFina):
							resp = s.get(oscUrl)	
							if resp.status_code == 200:
								fi = open(oscFina, "wb")
								for chunk in resp.iter_content(chunk_size):
									fi.write(chunk)
								fi.close()

						stateUrl = "{0}{1:03d}.state.txt".format(url2,k)
						stateFina = "{0}{1:03d}.state.txt".format(localpath2,k)
						if not os.path.exists(stateFina):
							resp = s.get(stateUrl)	
							if resp.status_code == 200:
								fi = open(stateFina, "wb")
								for chunk in resp.iter_content(chunk_size):
									fi.write(chunk)
								fi.close()

						sleepTime = 60


			done = True

		except requests.exceptions.ConnectionError as err:
			print err
			sleepTime *= 2
			time.sleep(sleepTime)

