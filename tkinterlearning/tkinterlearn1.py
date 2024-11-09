import tkinter as tk
from tkinter import *


window = tk.Tk() # makes window

window.geometry("400x300")

def submit():
    print("Box pressed")

frame1 = Frame(relief="sunken",borderwidth=5, bg="white")
    
button1 = Button(text = "Today's Weather",
                  foreground = "white",
                  background = "black",
                  width="32",
                  height="3",
                  command=submit,
                  master=frame1)
button1.pack(padx=5,pady=5)

button2 = Button(text = "Forecast",
                  foreground = "white",
                  background = "black",
                  width="32",
                  height="3",
                  command=submit,
                  master=frame1)
button2.pack(padx=5,pady=5)

button3 = Button(text = "Last week's weather",
                  foreground = "white",
                  background = "black",
                  width="32",
                  height="3",
                  command=submit,
                  master=frame1)
button3.pack(padx=5,pady=5)

button4 = Button(text = "Data search",
                  foreground = "white",
                  background = "black",
                  width="32",
                  height="3",
                  command=submit,
                  master=frame1)
button4.pack(padx=5,pady=5)

frame1.pack()

window.mainloop() # this runs window infintely, and listens for events



