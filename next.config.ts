/** @type {import('next').NextConfig} */
const nextConfig = {
  // Cấp phép cho IP điện thoại của bạn truy cập
  allowedDevOrigins: ['192.168.1.3', 'http://192.168.1.3:3000'],
};

export default nextConfig;