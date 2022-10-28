import os
import base64
import string
import random
import logging
from datetime import datetime
import shutil
from ..k8s import FalcoServingKube


TYPE_MAP = {"int": int, "str": str, "float": float, "list": list}


def check_type(variable, str_type):
	return type(variable) == TYPE_MAP[str_type]


def try_cast(variable, str_type):
	try:
		variable = TYPE_MAP[str_type](variable)
		return variable
	except:
		return False

def array_to_base64(arr):
	return base64.b64encode(arr)


def message(status, message):
	response_object = {"status": status, "message": message}
	return response_object


def validation_error(status, errors):
	response_object = {"status": status, "errors": errors}

	return response_object


def err_resp(msg, reason, code):
	err = message(False, msg)
	err["error_reason"] = reason
	return err, code


def internal_err_resp():
	err = message(False, "Something went wrong during the process!")
	err["error_reason"] = "server_error"
	return err, 500

def mkdir(path):
	try:
		logging.info(f"mkdir is called with {path}")
		path = os.path.relpath(path)
		if exists(path):
			return
		os.makedirs(path)
	except Exception as e:
		logging.error(e)

def put(f_from, f_to):
	logging.info(f"Local put: copying from {f_from} to {f_to}")
	shutil.copy2(f_from, f_to)

def rmtree(path):
	path = os.path.relpath(path)
	if not path.endswith("/"):
		path = path + "/"
	shutil.rmtree(path)

def random_string(N=6):
	randomstr = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
	logging.info(f"random_string called with N={N}: returning {randomstr}")
	return randomstr

def tempdir():
	import platform
	import tempfile
	tempdir = "/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()
	logging.info(f"tempdir called: return {tempdir}")
	return tempdir

def download_file(filename):
	return filename


def exists(path):
	return os.path.exists(path)

def rm_file(filename):

	logging.info(f"Removing {filename}")	
	# TODO: this function is being called by different purposes 
	# must be refactored
	os.remove(filename)
		
def get_service(service_name):
	deployment = os.getenv("DEPLOYMENT","local")

	kube = FalcoServingKube(service_name)
	URL = f"http://" + kube.get_service_address(external=deployment=="local", 
		hostname=deployment=="local")
	return URL