import tkinter as tk
import HadoopController as hc

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
        tk.Button(self, text="Movie Prediction", width=25, command=lambda: master.switch_frame(MovieRec)).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="K Nearest Neighbours", width=25, command=lambda: hc.get_knn() ).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="Pearson Correlation", width=25, command=lambda: master.switch_frame(PearsonCorr)).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="Average Movie Rating", width=25, command=lambda: master.switch_frame(AvgMovieRating)).pack(fill=tk.BOTH, expand=True)
        tk.Button(self, text="Average User Rating", width=25, command=lambda: master.switch_frame(AvgUserRating)).pack(fill=tk.BOTH, expand=True)

class MovieRec(tk.Frame):
    def __init__(self, master):
        tk.Frame.__init__(self, master)
        tk.Frame.configure(self, bg='#ffffff')
        tk.Label(self, text="Movie Rating Prediction", font=('Helvetica', 18, "bold")).pack(side="top", fill="x", pady=5)
        text = tk.Text(self, width=25, height=1)
        text.insert(tk.END, hc.get_movie_rec())
        text.pack()
        tk.Button(self, text="Go back to menu", command=lambda: master.switch_frame(Menu)).pack()

class PearsonCorr(tk.Frame):
    def __init__(self, master):
        tk.Frame.__init__(self, master)
        tk.Frame.configure(self, bg='#ffffff')
        tk.Label(self, text="Find Pearson Correlations Between Users", font=('Helvetica', 18, "bold")).pack(side="top", fill="x", pady=5)
        tk.Label(self, text="First User Id", bg="#fff", font=('Helvetica', 14, "bold")).pack()
        self.user_id_1 = tk.Entry(self)
        self.user_id_1.pack()
        tk.Label(self, text="Second User Id", bg="#fff", font=('Helvetica', 14, "bold")).pack()
        self.user_id_2 = tk.Entry(self)
        self.user_id_2.pack()
        self.text=None
        tk.Button(self, text="Find", command=lambda: self.show_pearson_corrs()).pack()
        tk.Button(self, text="Go back to menu", command=lambda: master.switch_frame(Menu)).pack()

    def show_pearson_corrs(self):
        if(self.text is not None):
            self.text.destroy()
        userId1 = SampleApp.get_entry_data(self.user_id_1)
        userId2 = SampleApp.get_entry_data(self.user_id_2)

        df = hc.get_pearson_corrs()
        data = float(df.loc[(df["userId1"] == int(userId1)) & (df["userId2"] == int(userId2))]['corr'])
        self.text = tk.Text(self, width=25, height=1)
        self.text.insert(tk.END, "Pearson(" + userId1 + "," + userId2 + ") ---> " + str(data))
        self.text.pack()


class AvgMovieRating(tk.Frame):
    def __init__(self, master):
        tk.Frame.__init__(self, master)
        tk.Frame.configure(self, bg='#ffffff')
        tk.Label(self, text="Find Average Movie Rating", font=('Helvetica', 18, "bold")).pack(side="top", fill="x", pady=5)
        tk.Label(self, text="Movie Id", bg="#fff", font=('Helvetica', 14, "bold")).pack()
        self.movie_id = tk.Entry(self)
        self.movie_id.pack()
        self.text=None
        tk.Button(self, text="Find", command=lambda: self.show_avg_rating()).pack()
        tk.Button(self, text="Go back to menu", command=lambda: master.switch_frame(Menu)).pack()

    def show_avg_rating(self):
        if(self.text is not None):
            self.text.destroy()
        movieId = SampleApp.get_entry_data(self.movie_id)
        df = hc.get_movie_avg_ratings()
        data = float(df.loc[df['movieId'] == int(movieId)]['movieAvgRating'])
        self.text = tk.Text(self, width=25, height=1)
        self.text.insert(tk.END, movieId + " ---> " + str(data))
        self.text.pack()

class AvgUserRating(tk.Frame):
    def __init__(self, master):
        tk.Frame.__init__(self, master)
        tk.Frame.configure(self, bg='#ffffff')
        tk.Label(self, text="Find Average User Rating", font=('Helvetica', 18, "bold")).pack(side="top", fill="x", pady=5)
        tk.Label(self, text="User Id", bg="#fff", font=('Helvetica', 14, "bold")).pack()
        self.user_id = tk.Entry(self)
        self.user_id.pack()
        self.text=None
        #tk.Button(self, text="Find", command=lambda: SampleApp.get_entry_data(user_id)).pack()
        tk.Button(self, text="Find", command=lambda: self.show_avg_rating()).pack()
        tk.Button(self, text="Go back to menu", command=lambda: master.switch_frame(Menu)).pack()


    def show_avg_rating(self):
        if(self.text is not None):
            self.text.destroy()
        userId = SampleApp.get_entry_data(self.user_id)
        df = hc.get_user_avg_ratings()
        data = float(df.loc[df['userId'] == int(userId) ]['avgRating'])
        self.text = tk.Text(self, width=25, height=1)
        self.text.insert(tk.END, userId + " ---> " + str(data))
        self.text.pack()


if __name__ == "__main__":
    app = SampleApp()
    app.mainloop()
