/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  async rewrites() {
    // Ensure we get the runtime environment variable
    const apiUrl = process.env.API_URL || 'http://automated-api:8101'
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