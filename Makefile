# Variables
CC = gcc
CFLAGS = -Wall -O2
LDFLAGS = 
LIBS = -lrdmacm -libverbs # You may need to link against RDMA related libraries or others.
TARGET = client
SRC = client.c # Assuming your source file is named client.c. Change this as needed.
OBJ = $(SRC:.c=.o)

# All targets
all: client server

# Target for client
client: client.o
	$(CC) $(CFLAGS) -o client client.o $(LIBS)

client.o: client.c
	$(CC) $(CFLAGS) -c client.c

# Target for server
server: server.o
	$(CC) $(CFLAGS) -o server server.o $(LIBS)

server.o: server.c
	$(CC) $(CFLAGS) -c server.c

# Clean up
clean:
	rm -f client server *.o

.PHONY: all clean