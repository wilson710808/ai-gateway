FROM node:18-alpine

WORKDIR /app

# 安装依赖
COPY server/package*.json ./
RUN npm ci --only=production

# 复制代码
COPY server/ ./

# 创建数据目录
RUN mkdir -p /app/data/logs /app/data/weekly

# 切换到非 root 用户
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001 && \
    chown -R nodejs:nodejs /app

USER nodejs

EXPOSE 3000 3001

CMD ["node", "index.js"]
