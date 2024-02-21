from threading import Thread

counter = 0


def increase():
    global counter
    for i in range(0, 1000000):
        counter = counter + 1


threads = []
for i in range(0, 400):
    threads.append(Thread(target=increase))
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()

print(f"Final counter: {counter}")
