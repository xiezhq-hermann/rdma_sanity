# Variables
CC = gcc
CFLAGS = -Wall -O2
LDFLAGS = 
LIBS = -lrdmacm -libverbs # You may need to link against RDMA related libraries or others.
TARGET = client
SRC = client.c # Assuming your source file is named client.c. Change this as needed.
OBJ = $(SRC:.c=.o)

# Default target
all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# Clean target
clean:
	rm -f $(OBJ) $(TARGET)

# Phony targets
.PHONY: all clean