# 🎵 Music Share - Real-Time Audio Streaming

Deploy to Render with Appwrite storage.

## 🚀 Deployment Steps

### 1. Setup Appwrite

1. Go to [Appwrite Cloud](https://cloud.appwrite.io)
2. Create a new project
3. Create a Storage Bucket:
   - Go to Storage → Create Bucket
   - Name it "music-files"
   - Set permissions: Allow public read access
   - Note the Bucket ID
4. Get API Key:
   - Go to Settings → API Keys
   - Create new API key with Storage permissions
   - Copy the API key

### 2. Deploy to Render

1. Push code to GitHub
2. Go to [Render Dashboard](https://dashboard.render.com)
3. Click "New +" → "Web Service"
4. Connect your GitHub repository
5. Configure:
   - **Name**: music-share-app
   - **Environment**: Node
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
   - **Plan**: Free

### 3. Add Environment Variables in Render

Go to Environment tab and add:

