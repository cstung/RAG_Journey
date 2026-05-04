import { useState } from "react"

export default function App() {
  const [message, setMessage] = useState("")
  const [output, setOutput] = useState("")

  const ask = async () => {
    const res = await fetch("/chat/stream", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({message, language:"vi"})})
    const text = await res.text()
    setOutput(text)
  }

  return <main style={{padding:20}}>
    <h1>LWHN Rebuild</h1>
    <textarea value={message} onChange={(e)=>setMessage(e.target.value)} />
    <button onClick={ask}>Send</button>
    <pre>{output}</pre>
  </main>
}
