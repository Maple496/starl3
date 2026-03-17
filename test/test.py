from melo.api import TTS
model = TTS(language='ZH', device='auto')
speaker_ids = model.hps.data.spk2id
model.tts_to_file("你好世界", speaker_ids['ZH'], "output.wav", speed=1.0)