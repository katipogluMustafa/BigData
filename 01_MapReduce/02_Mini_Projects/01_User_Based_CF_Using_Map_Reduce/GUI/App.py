import tkinter as tk
import HdfsExplorer as explorer
import HadoopRunner as runner

class SampleApp(tk.Tk):
    def __init__(self):
        tk.Tk.__init__(self)
        self._frame = None
        self.switch_frame(Menu)
        self.geometry("720x480")

    def switch_frame(self, frame_class):
        new_frame = frame_class(self)
        if self._frame is not None:
            self._frame.destroy()
        self._frame = new_frame
        self._frame.pack(fill=tk.BOTH, expand=True)

    @staticmethod
    def get_entry_data(entry_object):
        return entry_object.get()

class Menu(tk.Frame):
    def __init__(self, master):
        tk.Frame.__init__(self, master)
        tk.Button(self, text="Movie Prediction", width=25).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="K Nearest Neighbours", width=25).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="Pearson Correlation", width=25).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="Find Movie Watchers", width=25).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="Average Movie Rating", width=25).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="Average User Rating", width=25, command=lambda: master.switch_frame(AvgUserRating)).pack(fill=tk.BOTH, expand=True)


class AvgUserRating(tk.Frame):
    def __init__(self, master):
        tk.Frame.__init__(self, master)
        tk.Frame.configure(self, bg='#ffffff')
        tk.Label(self, text="Find Average User Rating", font=('Helvetica', 18, "bold")).pack(side="top", fill="x", pady=5)
        tk.Label(self, text="User Id", bg="#fff", font=('Helvetica', 14, "bold")).pack()
        user_id = tk.Entry(self)
        user_id.pack()
        tk.Button(self, text="Find", command=lambda: SampleApp.get_entry_data(user_id)).pack()
        tk.Button(self, text="Go back to menu", command=lambda: master.switch_frame(Menu)).pack()

    def show_avg_rating(self, user_id):
        pass

if __name__ == "__main__":
    app = SampleApp()
    app.mainloop()