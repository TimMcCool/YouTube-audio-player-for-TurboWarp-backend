import scratchattach as sa
from threading import Thread
import os
from subprocess import Popen, PIPE
import numpy as np
from PIL import Image
import subprocess
from pytubefix import YouTube
import time
import math
from scratchattach import Encoding
import dhooks
import json
import requests
from bs4 import BeautifulSoup

# file paths
source_path = "source.wav"
freqs_bmp_path = "freqs.bmp"
freqs_txt_path = "reqs.txt"

project_id = 864001370
packet_length = 45 # amount of data points sent in one cloud variable set

synth_queue = [] # users queued for synthesizing

num_running_dl = set() # user that are currently running a synthesizing process
symb = " !#$%&'()*+-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`爱北京长大耳风高汉花家看乐美南能欧平人三四天万小喜有正雪语言友纸子中{|}~üöäßûôâ"[:104] #symbols used for encoding the result
letters = list(symb)

def clean_up(uid):
    # removes all files and data related to the user with user id: uid
    global num_running_dl

    files_to_delete = [
        f'source_{uid}.wav',
        f'source_L_{uid}.wav',
        f"source_R_{uid}.wav",
        f"source_slice_{uid}.wav"
    ]

    for file_name in files_to_delete:
        try:
            if os.path.exists(file_name):
                os.remove(file_name)
                print(f"Deleted: {file_name}")
            else:
                print(f"File not found: {file_name}")
        except PermissionError:
            print(f"Permission denied: {file_name}")
        except Exception as e:
            print(f"Error deleting {file_name}: {e}")

    if uid in num_running_dl:
        num_running_dl.remove(uid)

def encode(inp, *, max_length):
    inp = str(inp)
    global encode_letters
    outp = ""
    count = 0
    for i in inp:
        count += 1
        if count > max_length:
            break
        if i in letters:
            if len(str(letters.index(i))) == 2:
                outp = f"{outp}{letters.index(i)}"
            elif len(str(letters.index(i))) == 1:
                outp = f"{outp}0{letters.index(i)}"
            else:
                outp += "99"
        else:
            outp += "00"
    return outp

def download_video_as_wav(youtube_url, channels, uid):
    # downloads the mp3 of a YouTube video and then converts it to wav
    global send_to_from_host_1, send_to_any, num_running_dl
        
    def get_cookies(cookies_jar):
        response_cookies = {}
        for c in cookies_jar:
            response_cookies[c.name] = c.value
        xsrf_token = response_cookies.get('XSRF-TOKEN')
        y2mate_session = response_cookies.get('y2mate_session')
        return xsrf_token, y2mate_session

    video_id = youtube_url  # video id

    # google cookies
    ga_cookie = 'GA1.1.1006377416.1724052228'
    ga_longer_cookie = 'GS1.1.1724052227.1.1.1724052281.0.0.0'

    # Global headers that are reused
    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,pl;q=0.7',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'dnt': '1',
        'origin': 'https://y2mate.is',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://y2mate.is/watch?v={video_id}',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    }

    params = {
        'v': video_id,
    }

    print('opening: https://y2mate.is/watch')
    response = requests.get('https://y2mate.is/watch', params=params, headers=headers)

    response_cookies = get_cookies(response.cookies)
    xsrf_token, y2mate_session = response_cookies
    csrf_token = BeautifulSoup(response.text, 'html.parser').find('meta', attrs={'name': 'csrf-token'})['content']

    cookies = {
        '_ga': ga_cookie,
        '_ga_MRLTGEXL5X': ga_longer_cookie,
        'XSRF-TOKEN': xsrf_token,
        'y2mate_session': y2mate_session,
    }

    # Update headers with X-CSRF-Token for the next requests
    headers['x-csrf-token'] = csrf_token
    headers['x-requested-with'] = 'XMLHttpRequest'

    data = {
        'url': f'https://www.youtube.com/watch?v={video_id}',
    }

    print('opening: https://y2mate.is/analyze')
    response = requests.post('https://y2mate.is/analyze', cookies=cookies, headers=headers, data=data)

    hash_value = response.json()['formats']['audio'][4]['hash']

    response_cookies = get_cookies(response.cookies)
    xsrf_token, y2mate_session = response_cookies

    cookies = {
        '_ga': ga_cookie,
        '_ga_MRLTGEXL5X': ga_longer_cookie,
        'XSRF-TOKEN': xsrf_token,
        'y2mate_session': y2mate_session,
    }

    data = {
        'hash': hash_value,
    }

    print('opening: https://y2mate.is/convert')
    response = requests.post('https://y2mate.is/convert', cookies=cookies, headers=headers, data=data)

    while 'download' not in response.json():
        time.sleep(0.5)
        response_cookies = get_cookies(response.cookies)
        xsrf_token, y2mate_session = response_cookies
        task_id = response.json()['taskId']

        cookies = {
            '_ga': ga_cookie,
            '_ga_MRLTGEXL5X': ga_longer_cookie,
            'XSRF-TOKEN': xsrf_token,
            'y2mate_session': y2mate_session,
        }

        data = {
            'taskId': task_id,
        }

        print('opening: https://y2mate.is/task')
        response = requests.post('https://y2mate.is/task', cookies=cookies, headers=headers, data=data)

    download_link = response.json()['download']
    response = requests.get(download_link)

    with open(f"source_{uid}.mp3", 'wb') as f:
        f.write(response.content)

    '''# Download YouTube video # old downloading process, cant be used anymore due to youtube ratelimiting
    yt = YouTube("https://www.youtube.com/watch?v="+youtube_url)
    stream = yt.streams.filter(only_audio=True, file_extension='mp4').first()
    audio_file_path = stream.download("", filename=f'source_{uid}.mp3')'''
    #send_to_from_host_1.append("2"+str(uid))

    subprocess.call(["ffmpeg", "-i", source_path[:-4]+f"_{uid}.mp3", "-t", "600", source_path[:-4]+f"_temp2_{uid}.wav"])#[:-4]+"_temp2.wav"])
    print("before 2nd ffmpeg op")
    os.remove(source_path[:-4]+f"_{uid}.mp3")
	
    # converting to WAV and splitting channels (when stereo is activated)
    if channels== 2:
        subprocess.run([
        "ffmpeg",
        "-i", source_path[:-4] + f"_temp2_{uid}.wav",  # Input file path
        "-ac", "1", "-af", "pan=mono|c0=c0",
        source_path[:-4] + f"_temp3_L_{uid}.wav", # Output file path with "_left" suffix
        "-y"
        ])
        print("after 2nd ffmpeg op")
        subprocess.run([
        "ffmpeg",
        "-i", source_path[:-4] + f"_temp2_{uid}.wav",  # Input file path
        "-ac", "1", "-af", "pan=mono|c0=c1",
        source_path[:-4] + f"_temp3_R_{uid}.wav", # Output file path with "_left" suffix
        "-y"
        ])
        print("after 3rd ffmpeg op")
        os.remove(source_path[:-4]+f"_temp2_{uid}.wav")
        subprocess.call(["ffmpeg", "-i", source_path[:-4]+f"_temp3_L_{uid}.wav", "-c", "copy", "-bitexact","-map_metadata","-1", source_path[:-4] + f"_L_{uid}.wav", "-y"])
        print("after 4th ffmpeg op")
        os.remove(source_path[:-4]+f"_temp3_L_{uid}.wav")
        subprocess.call(["ffmpeg", "-i", source_path[:-4]+f"_temp3_R_{uid}.wav", "-c", "copy", "-bitexact","-map_metadata","-1", source_path[:-4] + f"_R_{uid}.wav", "-y"])
        print("after 5th ffmpeg op")
        os.remove(source_path[:-4]+f"_temp3_R_{uid}.wav")
    elif channels == 1:
        subprocess.call(["ffmpeg", "-i", source_path[:-4]+f"_temp2_{uid}.wav", "-c", "copy", "-bitexact","-map_metadata","-1", source_path[:-4]+f"_{uid}.wav", "-y"])
        print("after 2nd ffmpeg op")
        os.remove(source_path[:-4]+f"_temp2_{uid}.wav")
        
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'json',
        source_path[:-4]+f"_{uid}.wav"
    ]
	
    # get audio length (fails at some audio files for some reason, not debugged yet):
    try:
        # Run the command
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Parse the JSON output
        result_json = json.loads(result.stdout)

        # Extract the duration
        duration = int(result_json['format']['stdout'])

        return duration
    except Exception:
        return 600


def process_download(video_id, pix_ps, bands_po, channels, uid):
    # runs the whole downloading and audio synthesizing process for one user while balancing the load so the cpu usage isn't exceeded
    global active_users, active_users_last_time, send_to_from_host_1, send_to_any, num_running_dl, send_to_any_source, synth_queue
	
    print("New process started, running dl", num_running_dl)

    # settings (received from the project)
    min_freq = 50
    bands_po = int(bands_po)
    max_freq = 12800
    pix_ps = int(pix_ps)

    try:
        length = download_video_as_wav(video_id, int(channels), uid)
    except Exception:
        # video download failed
        send_to_from_host_1.append("-1"+str(uid))
        active_users.remove(uid)
        active_users_last_time.pop(uid)
        if uid in num_running_dl:
            num_running_dl.remove(uid)
        return

    if len(num_running_dl) >= 2:
        # wait until less than two synthesisis processes are running
        send_to_from_host_1.append("9"+str(uid))
        synth_queue.append(uid)
        time0 = int(time.time())

        while len(num_running_dl) >=2 or synth_queue[0] != uid or time.time() - time0 > 60:
            if time.time() - active_users_last_time[uid] > 7.5:
                active_users.remove(uid)
                active_users_last_time.pop(uid)
                print("Connection to client lost", uid)
                synth_queue.remove(uid)
                clean_up(uid)
                return
            time.sleep(0.25)

        synth_queue.remove(uid)

    num_running_dl.add(uid)
    send_to_from_host_1.append("3"+str(length*pix_ps).zfill(6)+"."+str(uid))

    freqs_L, freqs_R = [], []

    i = 0
    # slices audio in parts a 20 seconds, synthesizes them seperately and streams the result:
    for time_slice in range(0, 600, 20):
        if time_slice > length:
            if uid in num_running_dl:
                num_running_dl.remove(uid)
                break
        try:
            # synthesize audio with ARSS:
            if int(channels) == 2:
                subprocess.call(["ffmpeg", "-i", source_path[:-4]+f"_L_{uid}.wav", "-ss", str(time_slice), "-t", str(20), "-c", "copy", "-bitexact", source_path[:-4]+f"_slice_{uid}.wav", "-y"])

                # run ARSS
                p = Popen(f'arss -g=1 --log-base=2 "{source_path[:-4]}_slice_{uid}.wav" "{freqs_bmp_path}"', stdin=PIPE, text=True, shell=True)
                p.communicate(os.linesep.join([str(min_freq), str(bands_po), str(max_freq), str(pix_ps)]))
                # convert to txt
                spectro = np.asarray(Image.open(freqs_bmp_path).convert("L")).T

                with open(freqs_txt_path, "w", encoding='utf-8') as f:
                    for line in spectro:
                        line = np.copy(line).astype(np.float32)[8*bands_po::-1]
                        line /= 255
                        line *= len(symb) - 1
                        line = list(line.astype(np.uint8))
                        #line = "".join([str(x).zfill(2)[:2] for x in line])
                        line = "".join([symb[x] for x in line])
                        freqs_L.append(line)
                        print(line, file=f)

                subprocess.call(["ffmpeg", "-i", source_path[:-4]+f"_R_{uid}.wav", "-ss", str(time_slice), "-t", str(20), "-c", "copy", "-bitexact", source_path[:-4]+f"_slice_{uid}.wav", "-y"])

                # run ARSS
                p = Popen(f'arss -g=1 --log-base=2 "{source_path[:-4]}_slice_{uid}.wav" "{freqs_bmp_path}"', stdin=PIPE, text=True, shell=True)
                p.communicate(os.linesep.join([str(min_freq), str(bands_po), str(max_freq), str(pix_ps)]))


                # convert to txt
                spectro = np.asarray(Image.open(freqs_bmp_path).convert("L")).T

                with open(freqs_txt_path, "w", encoding='utf-8') as f:
                    for line in spectro:
                        line = np.copy(line).astype(np.float32)[8*bands_po::-1]
                        line /= 255
                        line *= len(symb) - 1
                        line = list(line.astype(np.uint8))
                        #line = "".join([str(x).zfill(2)[:2] for x in line])
                        line = "".join([symb[x] for x in line])
                        freqs_R.append(line)
                        print(line, file=f)#
            elif int(channels) == 1:

                subprocess.call(["ffmpeg", "-i", source_path[:-4]+f"_{uid}.wav", "-ss", str(time_slice), "-t", str(20), "-c", "copy", "-bitexact", source_path[:-4]+f"_slice_{uid}.wav", "-y"])
                # run ARSS
                p = Popen(f'arss -g=1 --log-base=2 "{source_path[:-4]}_slice_{uid}.wav" "{freqs_bmp_path}"', stdin=PIPE, text=True, shell=True)
                p.communicate(os.linesep.join([str(min_freq), str(bands_po), str(max_freq), str(pix_ps)]))
                # convert to txt
                spectro = np.asarray(Image.open(freqs_bmp_path).convert("L")).T

                with open(freqs_txt_path, "w", encoding='utf-8') as f:
                    for line in spectro:
                        line = np.copy(line).astype(np.float32)[8*bands_po::-1]
                        line /= 255
                        line *= len(symb) - 1
                        line = list(line.astype(np.uint8))
                        #line = "".join([str(x).zfill(2)[:2] for x in line])
                        line = "".join([symb[x] for x in line])
                        freqs_L.append(line)
                        print(line, file=f)

            print("finished ARSS")

            send_to_from_host_1.append("4"+"."+str(uid))

        except Exception as e:
            print("synth error", e)
            if uid in num_running_dl:
                num_running_dl.remove(uid)
            send_to_from_host_1.append("-1"+str(uid))
            active_users.remove(uid)
            active_users_last_time.pop(uid)
            clean_up(uid)
            return

        if uid in num_running_dl:
            num_running_dl.remove(uid)

        # Clean up:
        # List of file names to delete
        files_to_delete = [
            f'source_temp2_{uid}.wav',
            f'source_{uid}.mp3',
            f"source_temp3_L_{uid}.wav",
            f"source_temp3_R_{uid}.wav"
        ]

        for file_name in files_to_delete:
            try:
                if os.path.exists(file_name):
                    os.remove(file_name)
                    print(f"Deleted: {file_name}")
                else:
                    print(f"File not found: {file_name}")
            except PermissionError:
                print(f"Permission denied: {file_name}")
            except Exception as e:
                print(f"Error deleting {file_name}: {e}")

        active_users_last_time[uid] = time.time()

        while True:
            # stream audio while balancing the load:
            if i+packet_length >= len(freqs_L):
                break

            if len(num_running_dl) > 0:
                time.sleep(0.08)
            value = "1"+str(i).zfill(5)+str(uid)+"."
            for o in range(packet_length):
                value += str(encode(freqs_L[i], max_length=bands_po*8))
                i += 1
            send_to_any.append(value+"1")
            send_to_any_source.append(uid)

            if time.time() - active_users_last_time[uid] > 7.5:
                active_users.remove(uid)
                active_users_last_time.pop(uid)
                print("Connection to client lost", uid)
                clean_up(uid)
                return

            if int(channels) == 2:
                i -= packet_length

                if i+packet_length >= len(freqs_R):
                    break

                value = "2"+str(i).zfill(5)+str(uid)+"."
                for o in range(packet_length):
                    value += str(encode(freqs_R[i], max_length=bands_po*8))
                    i += 1
                send_to_any.append(value+"1")
                send_to_any_source.append(uid)

            if time.time() - active_users_last_time[uid] > 7.5:
                active_users.remove(uid)
                active_users_last_time.pop(uid)
                clean_up(uid)
                print("Connection to client lost", uid)
                return
		
        # check if user left:
        if time_slice + 20 <= length:
            synth_queue.append(uid)
            while len(num_running_dl) >= 2 or synth_queue[0] != uid:
                time.sleep(0.1)
                if time.time() - active_users_last_time[uid] > 7.5:
                    synth_queue.remove(uid)
                    active_users.remove(uid)
                    active_users_last_time.pop(uid)
                    print("Connection to client lost", uid)
                    clean_up(uid)
                    return
            synth_queue.remove(uid)
            num_running_dl.add(uid)
    clean_up(uid)
    print("Finished process and cleaned up")
    while time.time() - active_users_last_time[uid] < 5:
        time.sleep(1)
    active_users.remove(uid)
    active_users_last_time.pop(uid)
    print("Connection to client lost", uid)

events = sa.WsCloudEvents(project_id, sa.connect_tw_cloud(project_id))

active_users = []
active_users_last_time = {}

@events.event
def on_ready():
    print("Events connected")

@events.event
def on_set(event):
    # record cloud activity
    global active_users_last_time, active_users, send_to_from_host_1
    if event.name == "request" and event.value != "0":
        user = event.value.split(".")[1]
        if user in active_users:
            if event.value.split(".")[0] == "2": # signal to continue transmission
                active_users_last_time[user] = time.time()
            if event.value.split(".")[0] == "3": # signal to stop transmission
                active_users_last_time.pop(user)
                active_users.remove(user)
        else:
            # signal to start new downloading process (user request)
            video_id, pix_ps, bands_po, channels  = Encoding.decode(event.value.split(".")[0]).split("|")
            video_id = video_id.replace("/", "")
            video_id = video_id.split("?v")[-1]
            print(user, "requests", "https://youtube.com/watch?v="+video_id)
            dhooks.Webhook("webhook link removed").send("https://youtube.com/watch?v="+video_id) # log data
            active_users.append(user)
            active_users_last_time[user] = time.time()
            send_to_from_host_1.append("1"+str(user))
            Thread(target=process_download, args=[video_id, pix_ps, bands_po, channels, user]).start() # start downloading and synthesizing process

def manage_cloud_var_sets():
    # sends back the data created by the process_download threads to the scratch project
    conn = sa.connect_tw_cloud(project_id, contact="TimMcCool", purpose="YouTube Music renderer")
    global send_to_from_host_1, send_to_any, send_to_any_source, active_users
    current_slot = 1
    while True:
        if send_to_from_host_1 != [] and current_slot == 1:
            conn.set_var("FROM_HOST_1", send_to_from_host_1.pop(0))
        elif send_to_any != []:
            source = send_to_any_source.pop(0)
            while source not in active_users:
                send_to_any.pop(0)
                if len(send_to_any_source) == 0:
                    break
                source = send_to_any_source.pop(0)
            else:
                conn.set_var(f"FROM_HOST_{current_slot}", send_to_any.pop(0))
        current_slot += 1
        if current_slot == 10:
            current_slot = 1
        if len(active_users) > 3:
            time.sleep(0.015)
        elif len(active_users) > 2:
            time.sleep(0.04)
        else:
            time.sleep(0.05)

send_to_from_host_1, send_to_any, send_to_any_source = [],[],[]

# start the backend:
Thread(target=manage_cloud_var_sets).start()
events.start(thread=False)
