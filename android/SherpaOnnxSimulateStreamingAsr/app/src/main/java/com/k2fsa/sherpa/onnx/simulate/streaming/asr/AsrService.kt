package com.k2fsa.sherpa.onnx.simulate.streaming.asr

import android.media.AudioFormat
import android.media.MediaRecorder
import android.media.AudioRecord
import android.speech.tts.SynthesisCallback
import android.speech.tts.SynthesisRequest
import android.speech.tts.TextToSpeech
import android.util.Log
import android.speech.RecognitionService
import android.speech.SpeechRecognizer
import com.k2fsa.sherpa.onnx.simulate.streaming.asr.SimulateStreamingAsr
import com.k2fsa.sherpa.onnx.simulate.streaming.asr.TAG
import android.content.Intent
import android.os.Bundle
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.channels.Channel
import android.content.Context
import android.content.ContextParams
import android.os.Build

class AsrService : RecognitionService() {

private var audioRecord: AudioRecord? = null
private val sampleRateInHz = 16000
private var samplesChannel = Channel<FloatArray>(capacity = Channel.UNLIMITED)
    private var isStarted  = false



    // we change asrModelType in github actions
    val asrModelType = 15



    override fun onStartListening(recognizerIntent: Intent?, listener: Callback?) {
        Log.d(TAG, "onStartListening")
        listener ?: return
        recognizerIntent ?: return

	        val attributionContext = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            this.createContext(
                ContextParams.Builder().setNextAttributionSource(listener.callingAttributionSource)
                    .build()
            )
        } else {
            this
        }
        SimulateStreamingAsr.initOfflineRecognizer(attributionContext, asrModelType)
        SimulateStreamingAsr.initVad(attributionContext.assets)
        listener.readyForSpeech(Bundle.EMPTY)
        isStarted = true

                val audioSource = MediaRecorder.AudioSource.MIC
                val channelConfig = AudioFormat.CHANNEL_IN_MONO
                val audioFormat = AudioFormat.ENCODING_PCM_16BIT
                val numBytes =
                    AudioRecord.getMinBufferSize(sampleRateInHz, channelConfig, audioFormat)
                audioRecord = AudioRecord(
                    audioSource,
                    sampleRateInHz,
                    AudioFormat.CHANNEL_IN_MONO,
                    AudioFormat.ENCODING_PCM_16BIT,
                    numBytes * 2 // a sample has two bytes as we are using 16-bit PCM
                )

                SimulateStreamingAsr.vad.reset()

                CoroutineScope(Dispatchers.IO).launch {
                    Log.i(TAG, "processing samples")
                    val interval = 0.1 // i.e., 100 ms
                    val bufferSize = (interval * sampleRateInHz).toInt() // in samples
                    val buffer = ShortArray(bufferSize)

                    audioRecord?.let { it ->
                        it.startRecording()

                        while (isStarted) {
                            val ret = audioRecord?.read(buffer, 0, buffer.size)
                            ret?.let { n ->
                                val samples = FloatArray(n) { buffer[it] / 32768.0f }
                                samplesChannel.send(samples)
                            }
                        }
                    }
                }

                CoroutineScope(Dispatchers.Default).launch {
                    var buffer = arrayListOf<Float>()
                    var offset = 0
                    val windowSize = 512
                    var isSpeechStarted = false
                    var startTime = System.currentTimeMillis()
                    var stopTime = System.currentTimeMillis()


                    while (isStarted) {
                        for (s in samplesChannel) {
                            if (s.isEmpty()) {
                                break
                            }

                            buffer.addAll(s.toList())
                            while (offset + windowSize < buffer.size) {
                                SimulateStreamingAsr.vad.acceptWaveform(
                                    buffer.subList(
                                        offset,
                                        offset + windowSize
                                    ).toFloatArray()
                                )
                                offset += windowSize
                                if (!isSpeechStarted && SimulateStreamingAsr.vad.isSpeechDetected()) {
                                    listener.beginningOfSpeech()
                                    isSpeechStarted = true
                                    startTime = System.currentTimeMillis()
                                }
                            }

                            val elapsed = System.currentTimeMillis() - startTime
                            if (isSpeechStarted && elapsed > 200) {
                                // Run ASR every 0.2 seconds == 200 milliseconds
                                // You can change it to some other value
                                val stream = SimulateStreamingAsr.recognizer.createStream()
                                stream.acceptWaveform(
                                    buffer.subList(0, offset).toFloatArray(),
                                    sampleRateInHz
                                )
                                SimulateStreamingAsr.recognizer.decode(stream)
                                val result = SimulateStreamingAsr.recognizer.getResult(stream)
                                stream.release()


                                    listener.partialResults(Bundle().apply {
                                        putStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION, arrayListOf(
					  result.text
                                        ))
                                    })

                                startTime = System.currentTimeMillis()
                            }


                            while (!SimulateStreamingAsr.vad.empty()) {
                                val stream = SimulateStreamingAsr.recognizer.createStream()
                                stream.acceptWaveform(
                                    SimulateStreamingAsr.vad.front().samples,
                                    sampleRateInHz
                                )
                                SimulateStreamingAsr.recognizer.decode(stream)
                                val result = SimulateStreamingAsr.recognizer.getResult(stream)
                                stream.release()

                                isSpeechStarted = false
                                stopTime = System.currentTimeMillis()
	listener.endOfSpeech()
                                SimulateStreamingAsr.vad.pop()

                                buffer = arrayListOf()
                                offset = 0

                                    listener.results(Bundle().apply {
                                        putStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION, arrayListOf(
					  result.text
                                        ))
                                    })
                            }
                    Log.i(TAG, "stop time $stopTime current ${System.currentTimeMillis()}")
			if (System.currentTimeMillis() - stopTime > 5000) {
				onStopListening(listener)
			}
                        }
                    }
                }

    }

    override fun onCancel(listener: Callback?) {
        Log.d(TAG, "onCancel")
Log.w(TAG,Log.getStackTraceString(Throwable()))
        isStarted = false
            audioRecord?.stop()
            audioRecord?.release()
            audioRecord = null
    }

    override fun onStopListening(listener: Callback?) {
        Log.d(TAG, "onStopListening")
        isStarted = false
            audioRecord?.stop()
            audioRecord?.release()
            audioRecord = null
    }





}
