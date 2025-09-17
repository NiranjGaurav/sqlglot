/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  async rewrites() {
    // Ensure we get the runtime environment variable
    const apiUrl = process.env.API_URL || 'http://transpiler:8100'
    console.log('Next.js rewrites using API_URL:', apiUrl)
    
    return [
      {
        source: '/api/:path*',
        destination: `${apiUrl}/:path*`,
      },
    ]
  },
}

module.exports = nextConfig