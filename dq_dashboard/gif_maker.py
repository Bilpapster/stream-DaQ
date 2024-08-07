import glob
import sys

from PIL import Image


def make_gif(frame_folder):
    frames = [Image.open(f"{frame_folder}/Figure_{number}.png") for number in range(1, 50 + 1)]
    frame_one = frames[0]
    frame_one.save("dq_dashboard.gif", format="GIF", append_images=frames, save_all=True, duration=1500, loop=0)


if __name__ == "__main__":
    make_gif("Figures")
