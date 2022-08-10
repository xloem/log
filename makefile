all: capture
%:%.c
	gcc $< -o $@ -ldl -lm -lpthread
