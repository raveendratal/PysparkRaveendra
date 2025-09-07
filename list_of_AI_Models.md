# 🤖 AI Model Performance & Pricing Index
This document compares leading AI models across intelligence, price, generation speed, and latency for cost, throughput, and responsiveness trade-offs in production systems.

---

## Columns
- Model – Model name & version
- Creator – Company/organization
- Context Window – Max tokens supported
- Intelligence Index – Benchmark-based capability score
- Price (USD / 1M tokens) – Blended price
- Output Tokens/s (Median) – Generation throughput
- Latency – First Chunk (s) – Delay to first token

---

## AI21 Labs
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟩 Jamba 1.7 Large | 🧠 AI21 Labs | 256k | 21 | $3.50 | 46.8 | 0.80 |
| 🟦 Jamba 1.7 Mini | 🧠 AI21 Labs | 258k | 4 | $0.25 | 139.3 | 0.66 |

## Alibaba
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟥 Qwen3 235B 2507 | 🏢 Alibaba | 256k | 57 | $2.63 | 52.6 | 1.15 |
| 🟥 Qwen3 30B 2507 | 🏢 Alibaba | 262k | 46 | $0.75 | 100.3 | 1.00 |
| 🟥 Qwen3 235B 2507 | 🏢 Alibaba | 256k | 45 | $1.23 | 34.4 | 1.11 |
| 🟥 Qwen3 4B 2507 | 🏢 Alibaba | 262k | 43 | $0.00 | 0.0 | 0.00 |
| 🟥 Qwen3 Coder 480B | 🏢 Alibaba | 262k | 42 | $3.00 | 42.3 | 1.56 |
| 🟥 QwQ-32B | 🏢 Alibaba | 131k | 38 | $0.74 | 47.5 | 0.61 |
| 🟥 Qwen3 30B 2507 | 🏢 Alibaba | 262k | 37 | $0.35 | 87.3 | 0.96 |
| 🟥 Qwen3 Coder 30B | 🏢 Alibaba | 262k | 33 | $0.90 | 92.2 | 1.46 |

## Amazon
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟧 Nova Premier | 🟠 Amazon | 1m | 31 | $5.00 | 74.4 | 0.88 |

## Anthropic
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟣 Claude 4.1 Opus | 🟪 Anthropic | 200k | 59 | $30.00 | 46.3 | 1.55 |
| 🟣 Claude 4 Sonnet | 🟪 Anthropic | 1m | 57 | $6.00 | 51.5 | 1.13 |
| 🟣 Claude 4 Opus | 🟪 Anthropic | 200k | 54 | $30.00 | 52.2 | 1.57 |
| 🟣 Claude 4.1 Opus | 🟪 Anthropic | 200k | 45 | $30.00 | 45.2 | 1.57 |
| 🟣 Claude 4 Sonnet | 🟪 Anthropic | 1m | 44 | $6.00 | 76.2 | 1.25 |
| 🟣 Claude 4 Opus | 🟪 Anthropic | 200k | 42 | $30.00 | 52.2 | 1.95 |

## Cohere
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟤 Command A | 🔶 Cohere | 256k | 28 | $4.38 | 94.0 | 0.18 |
| 🟤 Aya Expanse 32B | 🔶 Cohere | 128k | 6 | $0.75 | 69.5 | 0.12 |
| 🟤 Aya Expanse 8B | 🔶 Cohere | 8k | 2 | $0.75 | 81.0 | 0.19 |

## DeepSeek
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| ⚫ DeepSeek V3.1 | ⚙️ DeepSeek | 128k | 54 | $0.96 | 20.1 | 2.89 |
| ⚫ DeepSeek R1 0528 | ⚙️ DeepSeek | 128k | 52 | $0.96 | 20.5 | 2.84 |
| ⚫ DeepSeek V3.1 | ⚙️ DeepSeek | 128k | 45 | $0.48 | 19.8 | 2.83 |
| ⚫ DeepSeek R1 0528 Qwen3 8B | ⚙️ DeepSeek | 33k | 35 | $0.07 | 54.5 | 0.72 |

## Google (Gemini + Gemma)
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🔵 Gemini 2.5 Pro | 🔵 Google | 1m | 60 | $3.44 | 152.9 | 28.39 |
| 🔵 Gemini 2.5 Flash | 🔵 Google | 1m | 51 | $0.85 | 255.7 | 11.28 |
| 🔵 Gemini 2.5 Flash | 🔵 Google | 1m | 40 | $0.85 | 227.9 | 0.31 |
| 🔵 Gemini 2.5 Flash-Lite | 🔵 Google | 1m | 40 | $0.17 | 540.3 | 7.69 |
| 🔵 Gemini 2.5 Flash-Lite | 🔵 Google | 1m | 30 | $0.17 | 352.3 | 0.23 |
| 🔷 Gemma 3 27B | 🔷 Google | 128k | 22 | $0.00 | 47.1 | 0.63 |
| 🔷 Gemma 3 12B | 🔷 Google | 128k | 21 | $0.23 | 0.0 | 0.00 |
| 🔷 Gemma 3n E4B | 🔷 Google | 32k | 16 | $0.03 | 72.9 | 0.33 |
| 🔷 Gemma 3 4B | 🔷 Google | 128k | 15 | $0.03 | 0.0 | 0.00 |
| 🔷 Gemma 3n E2B | 🔷 Google | 32k | 8 | $0.00 | 43.1 | 0.32 |
| 🔷 Gemma 3 1B | 🔷 Google | 32k | 6 | $0.00 | 0.0 | 0.00 |

## IBM
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| ⚪ Granite 3.3 8B | ⚪ IBM | 128k | 15 | $0.09 | 109.9 | 0.41 |

## LG AI Research
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟩 EXAONE 4.0 32B | 🧩 LG AI Research | 131k | 43 | $0.70 | 77.0 | 0.30 |
| 🟩 EXAONE 4.0 32B | 🧩 LG AI Research | 131k | 33 | $0.70 | 68.2 | 0.33 |
| 🟩 Exaone 4.0 1.2B | 🧩 LG AI Research | 64k | 27 | $0.00 | 0.0 | 0.00 |
| 🟩 Exaone 4.0 1.2B | 🧩 LG AI Research | 64k | 20 | $0.00 | 0.0 | 0.00 |

## Liquid AI
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🔵 LFM2 1.2B | 💧 Liquid AI | 33k | 8 | $0.00 | 0.0 | 0.00 |

## Meta
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟪 Llama 4 Maverick | 🦙 Meta | 1m | 36 | $0.39 | 143.8 | 0.34 |
| 🦙 Llama 4 Scout | 🦙 Meta | 10m | 28 | $0.26 | 115.5 | 0.37 |
| 🦙 Llama 3.3 70B | 🦙 Meta | 128k | 28 | $0.60 | 85.2 | 0.45 |
| 🦙 Llama 3.1 405B | 🦙 Meta | 128k | 26 | $3.75 | 31.7 | 0.75 |
| 🦙 Llama 3.2 90B (Vision) | 🦙 Meta | 128k | 19 | $0.72 | 30.5 | 0.37 |
| 🦙 Llama 3.2 11B (Vision) | 🦙 Meta | 128k | 15 | $0.16 | 72.2 | 0.39 |

## Microsoft Azure
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🔷 Phi-4 | 🖥️ Microsoft Azure | 16k | 25 | $0.22 | 29.8 | 0.46 |
| 🔷 Phi-4 Multimodal | 🖥️ Microsoft Azure | 128k | 12 | $0.00 | 18.2 | 0.36 |

## MiniMax
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| ⬛ MiniMax M1 80k | ⚡ MiniMax | 1m | 46 | $0.82 | 0.0 | 0.00 |
| ⬛ MiniMax M1 40k | ⚡ MiniMax | 1m | 42 | $0.82 | 0.0 | 0.00 |
| ⬛ MiniMax-Text-01 | ⚡ MiniMax | 4m | 26 | $0.42 | 0.0 | 0.00 |

## Mistral
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🔺 Mistral Medium 3.1 | 🔺 Mistral | 128k | 35 | $0.80 | 48.8 | 0.41 |
| 🔺 Mistral Medium 3 | 🔺 Mistral | 128k | 35 | $0.80 | 67.7 | 0.43 |
| 🔺 Magistral Medium | 🔺 Mistral | 40k | 34 | $2.75 | 144.2 | 0.39 |
| 🔺 Magistral Small | 🔺 Mistral | 40k | 32 | $0.75 | 179.6 | 0.32 |
| 🔺 Mistral Small 3.2 | 🔺 Mistral | 128k | 29 | $0.15 | 144.5 | 0.28 |
| 🔺 Devstral Medium | 🔺 Mistral | 256k | 28 | $0.80 | 101.1 | 0.38 |
| 🔺 Devstral Small | 🔺 Mistral | 256k | 18 | $0.15 | 145.4 | 0.33 |
| 🔺 Codestral (Jan) | 🔺 Mistral | 256k | 13 | $0.45 | 152.6 | 0.30 |
| 🔺 Ministral 8B | 🔺 Mistral | 128k | 8 | $0.10 | 175.8 | 0.30 |
| 🔺 Ministral 3B | 🔺 Mistral | 128k | 5 | $0.04 | 248.1 | 0.29 |

## Moonshot AI
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🌙 Kimi K2 0905 | 🌙 Moonshot AI | 256k | 50 | $1.35 | 55.1 | 0.48 |
| 🌙 Kimi K2 | 🌙 Moonshot AI | 128k | 48 | $1.07 | 54.1 | 0.50 |

## Nous Research
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟣 Hermes 4 - Llama-3.1 70B | 🧠 Nous Research | 128k | 39 | $0.20 | 82.8 | 0.59 |
| 🟣 Hermes 4 405B | 🧠 Nous Research | 128k | 33 | $1.50 | 31.9 | 0.70 |
| 🟣 Hermes 4 70B | 🧠 Nous Research | 128k | 24 | $0.20 | 75.7 | 0.58 |
| 🟣 DeepHermes 3 - Mistral 24B | 🧠 Nous Research | 32k | 16 | $0.00 | 0.0 | 0.00 |
| 🟣 DeepHermes 3 - Llama-3.1 8B | 🧠 Nous Research | 128k | 2 | $0.00 | 0.0 | 0.00 |

## NVIDIA
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟢 Llama Nemotron Super 49B v1.5 | 🟩 NVIDIA | 128k | 45 | $0.00 | 0.0 | 0.00 |
| 🟢 Llama Nemotron Ultra | 🟩 NVIDIA | 128k | 38 | $0.90 | 37.5 | 0.71 |
| 🟢 NVIDIA Nemotron Nano 9B V2 | 🟩 NVIDIA | 131k | 38 | $0.00 | 0.0 | 0.00 |
| 🟢 NVIDIA Nemotron Nano 9B V2 | 🟩 NVIDIA | 131k | 37 | $0.00 | 0.0 | 0.00 |
| 🟢 Llama 3.3 Nemotron Super 49B | 🟩 NVIDIA | 128k | 35 | $0.00 | 0.0 | 0.00 |
| 🟢 Llama Nemotron Super 49B v1.5 | 🟩 NVIDIA | 128k | 27 | $0.00 | 0.0 | 0.00 |
| 🟢 Llama 3.1 Nemotron Nano 4B v1.1 | 🟩 NVIDIA | 128k | 26 | $0.00 | 0.0 | 0.00 |
| 🟢 Llama 3.3 Nemotron Super 49B v1 | 🟩 NVIDIA | 128k | 26 | $0.00 | 0.0 | 0.00 |
| 🟢 Llama 3.1 Nemotron 70B | 🟩 NVIDIA | 128k | 23 | $0.17 | 32.2 | 0.56 |

## OpenAI
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🤖 GPT-5 (high) | 🧭 OpenAI | 400k | 67 | $3.44 | 61.9 | 72.53 |
| 🤖 GPT-5 (medium) | 🧭 OpenAI | 400k | 66 | $3.44 | 177.3 | 30.76 |
| 🤖 o3-pro | 🧭 OpenAI | 200k | 65 | $35.00 | 20.1 | 127.04 |
| 🤖 o3 | 🧭 OpenAI | 200k | 65 | $3.50 | 187.1 | 12.32 |
| 🤖 GPT-5 mini (high) | 🧭 OpenAI | 400k | 62 | $0.69 | 76.7 | 99.19 |
| 🤖 GPT-5 (low) | 🧭 OpenAI | 400k | 62 | $3.44 | 150.9 | 14.41 |
| 🤖 GPT-5 mini (medium) | 🧭 OpenAI | 400k | 61 | $0.69 | 73.7 | 27.82 |
| 🤖 o4-mini (high) | 🧭 OpenAI | 200k | 59 | $1.93 | 107.9 | 39.30 |
| 🤖 gpt-oss-120B (high) | 🧭 OpenAI | 131k | 58 | $0.26 | 226.5 | 0.44 |
| 🤖 o3-mini (high) | 🧭 OpenAI | 200k | 51 | $1.93 | 141.7 | 41.54 |
| 🤖 GPT-5 nano (high) | 🧭 OpenAI | 400k | 49 | $0.14 | 122.8 | 67.06 |
| 🤖 o3-mini | 🧭 OpenAI | 200k | 48 | $1.93 | 149.3 | 12.46 |
| 🤖 GPT-5 nano (medium) | 🧭 OpenAI | 400k | 48 | $0.14 | 196.2 | 31.93 |
| 🤖 gpt-oss-20B (high) | 🧭 OpenAI | 131k | 45 | $0.09 | 255.5 | 0.52 |
| 🤖 GPT-5 (minimal) | 🧭 OpenAI | 400k | 43 | $3.44 | 134.0 | 1.05 |
| 🤖 GPT-4.1 | 🧭 OpenAI | 1m | 43 | $3.50 | 105.5 | 0.51 |
| 🤖 GPT-4.1 mini | 🧭 OpenAI | 1m | 42 | $0.70 | 70.5 | 0.45 |
| 🤖 GPT-5 mini (minimal) | 🧭 OpenAI | 400k | 42 | $0.69 | 73.1 | 0.98 |
| 🤖 GPT-5 nano (minimal) | 🧭 OpenAI | 400k | 29 | $0.14 | 209.0 | 0.85 |
| 🤖 GPT-4.1 nano | 🧭 OpenAI | 1m | 27 | $0.17 | 108.5 | 0.40 |
| 🤖 GPT-4o (ChatGPT) | 🧭 OpenAI | 128k | 25 | $7.50 | 0.0 | 0.00 |
| 🤖 GPT-4o mini | 🧭 OpenAI | 128k | 21 | $0.26 | 62.1 | 0.52 |

## Perplexity
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟣 R1 1776 | 🌀 Perplexity | 128k | 19 | $3.50 | 0.0 | 0.00 |

## Reka AI
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟠 Reka Flash 3 | 🔶 Reka AI | 128k | 33 | $0.35 | 50.9 | 1.31 |

## Upstage
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟡 Solar Pro 2 | ☀️ Upstage | 66k | 38 | $0.50 | 104.3 | 1.39 |
| 🟡 Solar Pro 2 | ☀️ Upstage | 66k | 30 | $0.50 | 107.1 | 1.39 |

## xAI
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟤 Grok 4 | ⚡ xAI | 256k | 65 | $6.00 | 49.0 | 11.64 |
| 🟤 Grok 3 mini Reasoning (high) | ⚡ xAI | 1m | 57 | $0.35 | 99.3 | 0.61 |
| 🟤 Grok Code Fast 1 | ⚡ xAI | 256k | 49 | $0.53 | 300.3 | 6.49 |
| 🟤 Grok 3 Reasoning Beta | ⚡ xAI | 1m | 41 | $0.00 | 0.0 | 0.00 |
| 🟤 Grok 3 | ⚡ xAI | 1m | 36 | $6.00 | 51.3 | 0.69 |
| 🟤 Grok 3 mini Reasoning (low) | ⚡ xAI | 1m | — | $0.35 | 62.0 | 0.57 |

## Z AI
| Model | Creator | Context Window | Intelligence Index | Price (USD / 1M tokens) | Output Tokens/s (Median) | Latency – First Chunk (s) |
|---|---|---:|---:|---:|---:|---:|
| 🟩 GLM-4.5 | 🧾 Z AI | 128k | 49 | $0.97 | 57.5 | 0.65 |
| 🟩 GLM-4.5-Air | 🧾 Z AI | 128k | 48 | $0.42 | 99.8 | 0.98 |
| 🟩 GLM-4.5V | 🧾 Z AI | 64k | 39 | $0.90 | 94.8 | 0.78 |
| 🟩 GLM-4.5V | 🧾 Z AI | 64k | 26 | $0.90 | 88.5 | 0.70 |
