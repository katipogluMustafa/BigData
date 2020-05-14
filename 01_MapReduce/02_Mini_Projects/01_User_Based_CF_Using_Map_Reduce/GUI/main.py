import tkinter as tk

window = tk.Tk()
window.title("Hadoop Movie Recommendation With Movielens")
window.geometry("720x480")

# tk.Label(window, text="movieId").grid(row=0)
# tk.Label(window, text="userId").grid(row=1)
#
# movieId = tk.Entry(window, width=10)
# movieId.grid(row=0, column=1)
# userId = tk.Entry(window, width=10)
# userId.grid(row=1, column=1)
#
# list_out = tk.Listbox(window)
#
# list_out.insert(1, 'SpiderMan')
# list_out.insert(2, 'Maze Runner')
# list_out.insert(3, 'Travellers')
# list_out.grid(row=3)
#
# msg = tk.Message(window, text="4.5 Rating")
# msg.config(bg='lightgreen')
# msg.grid(row=3,column=6)

def frame():
    f = tk.Frame(window)
    f.pack()

pane = tk.Frame(window)
pane.pack(fill=tk.BOTH, expand=True)

tk.Button(pane, text="Movie Prediction", width=25, command=window.destroy).pack(fill=tk.BOTH, expand=True)
tk.Button(pane, text="K Nearest Neighbours", width=25, command=window.destroy).pack(fill=tk.BOTH, expand=True)
tk.Button(pane, text="Pearson Correlation", width=25, command=window.destroy).pack(fill=tk.BOTH, expand=True)
tk.Button(pane, text="Find Movie Watchers", width=25, command=window.destroy).pack(fill=tk.BOTH, expand=True)
tk.Button(pane, text="Average Movie Rating", width=25, command=window.destroy).pack(fill=tk.BOTH, expand=True)
tk.Button(pane, text="Average User Rating", width=25, command=frame).pack(fill=tk.BOTH, expand=True)



window.mainloop()



