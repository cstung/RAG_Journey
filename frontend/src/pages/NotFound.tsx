import { Link } from 'react-router-dom'
import { Waves } from 'lucide-react'

export default function NotFound() {
  return (
    <div className="flex h-dvh flex-col items-center justify-center gap-4 text-center">
      <Waves className="h-12 w-12 text-ocean opacity-40" />
      <h1 className="font-display text-4xl font-extrabold text-ocean-deep">404</h1>
      <p className="text-muted-foreground">Trang không tồn tại.</p>
      <Link to="/" className="text-ocean underline underline-offset-2">
        Quay về trang chủ
      </Link>
    </div>
  )
}

