all: capture
%:%.c
	gcc $< -o $@ -ldl -lm -lpthread

dep: nonblocking-stream-queue.pypkg
%.pypkg:
	python3 -c 'from setuptools import setup; setup()' easy_install $*
