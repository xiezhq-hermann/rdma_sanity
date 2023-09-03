# Variables
CC = gcc
CFLAGS = -Wall -O2
LIBS = -lrdmacm -libverbs # You may need to link against RDMA related libraries or others.

# All targets
all: client server ib_sanity

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

# Target for server
ib_sanity: ib_sanity.o
	$(CC) $(CFLAGS) -o ib_sanity ib_sanity.o $(LIBS)

ib_sanity.o: ib_sanity.c
	$(CC) $(CFLAGS) -c ib_sanity.c


# Clean up
clean:
	rm -f client server ib_sanity *.o

.PHONY: all clean