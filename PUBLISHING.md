# Publishing PETadex Search to GitHub

## 📦 Complete Repo Structure Created

```
petadex-search/
├── lambda_function.py              # Lambda handler with S3 result storage
├── cli.py                          # Standalone CLI entrypoint
├── Dockerfile                      # Multi-mode container (Lambda + CLI)
├── requirements.txt                # Python dependencies
├── README.md                       # Comprehensive documentation
├── .gitignore                      # Git ignore file
├── .github/
│   └── workflows/
│       ├── docker-publish.yml      # Auto-publish to Docker Hub
│       └── update-database.yml.example  # Optional: auto database updates
└── scripts/
    ├── update_sequence_index.py    # Database update script
    ├── setup_s3_access.sh          # S3 setup helper
    └── README.md                   # Database documentation
```

---

## 🚀 Publishing Steps

### 1. Create GitHub Repository

```bash
# Initialize git (if not already done)
cd /Users/Pixel/Documents/projects/petadex-sequence-search
git init
git add .
git commit -m "Initial commit: Dual-mode MMseqs2 search engine"

# Create repo on GitHub, then:
git remote add origin git@github.com:yourusername/petadex-search.git
git branch -M main
git push -u origin main
```

### 2. Set Up Docker Hub Auto-Publishing

**A. Create Docker Hub Account** (if needed)
- Go to https://hub.docker.com
- Create account/sign in

**B. Generate Docker Hub Access Token**
1. Go to Account Settings → Security
2. Click "New Access Token"
3. Name: `github-actions`
4. Permissions: `Read, Write, Delete`
5. Copy the token (you won't see it again!)

**C. Add GitHub Secrets**
1. Go to your GitHub repo → Settings → Secrets and variables → Actions
2. Add two secrets:
   - `DOCKERHUB_USERNAME`: Your Docker Hub username
   - `DOCKERHUB_TOKEN`: The token you just created

### 3. Create First Release

```bash
# Tag a version
git tag -a v1.0.0 -m "Release v1.0.0: Dual-mode MMseqs2 search engine"
git push origin v1.0.0
```

**This automatically triggers the GitHub Action to build and push to Docker Hub!**

### 4. Update README with Your Docker Hub Username

Edit [README.md](README.md) and replace all instances of `yourusername` with your actual Docker Hub username.

```bash
# Example
docker pull yourrealusername/petadex-search:latest
```

---

## 🐳 Docker Hub Image

After publishing, your image will be available at:

```
https://hub.docker.com/r/yourusername/petadex-search
```

Users can pull and run with:

```bash
docker pull yourusername/petadex-search:latest
docker run --rm \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  yourusername/petadex-search \
  "MKLLIVLLAACLAVFAAAEPQIAVV" 10
```

---

## 🏷️ Versioning Strategy

Use semantic versioning:
- `v1.0.0` - Major release
- `v1.1.0` - New features
- `v1.0.1` - Bug fixes

Each tag automatically publishes to Docker Hub with matching tags:
- `yourusername/petadex-search:v1.0.0`
- `yourusername/petadex-search:latest` (from main branch)

---

## 📝 Pre-Publication Checklist

- [ ] Update README.md with your Docker Hub username
- [ ] Update README.md with your GitHub username
- [ ] Add a LICENSE file (MIT, Apache 2.0, GPL, etc.)
- [ ] Test Docker image builds locally
- [ ] Test CLI mode works
- [ ] Test Lambda mode works
- [ ] Set up Docker Hub secrets in GitHub
- [ ] Create first git tag and push

---

## 🔧 Testing Before Publishing

```bash
# Build image
docker build -t petadex-search-test .

# Test Lambda mode
docker run -p 9000:8080 \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  petadex-search-test

# Test CLI mode (in another terminal)
docker run --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  --entrypoint python3 \
  petadex-search-test cli.py "MKLLIVLLAACLAVFAAAEPQIAVV" 5
```

---

## 🎯 Next Steps After Publishing

1. **Add GitHub Topics**:
   - `bioinformatics`
   - `protein-search`
   - `mmseqs2`
   - `aws-lambda`
   - `docker`

2. **Create GitHub Release**:
   - Go to Releases → Draft a new release
   - Choose the tag you created
   - Add release notes

3. **Share on Social Media**:
   - Twitter/X with #bioinformatics
   - Reddit: r/bioinformatics
   - LinkedIn

4. **Submit to Package Indexes** (optional):
   - BioContainers
   - Bioconda
   - PyPI (if you create a pip package)

---

## 🔒 Security Notes

**DO NOT commit:**
- AWS credentials
- Database passwords
- `.env` files with secrets

These are already in `.gitignore`, but double-check before pushing.

---

## 📊 Monitoring After Publication

Track these metrics:
- **Docker Hub**: Pull counts, stars
- **GitHub**: Stars, forks, issues
- **Usage**: Lambda invocations, S3 bandwidth

---

## 💡 Grant-Worthy Framing

When describing in your grant application:

> "PETadex Search is an open-source, containerized sequence search engine that democratizes access to large-scale enzyme discovery. By publishing as both a standalone Docker container and an AWS Lambda function, we enable researchers worldwide to search against 217M+ enzyme sequences without requiring expensive computational infrastructure. The version-controlled architecture ensures reproducibility - a critical requirement for scientific research - while the dual-mode design supports both high-throughput cloud deployments and local development environments."

---

## 🆘 Troubleshooting

**GitHub Action fails to push to Docker Hub:**
- Check that secrets are set correctly
- Verify Docker Hub token has write permissions
- Check Docker Hub account is active

**Image too large:**
- Current size: ~2.5GB (acceptable for Lambda/Docker Hub)
- If needed, use multi-stage builds or alpine base

**CLI mode not working:**
- Ensure `--entrypoint python3` is specified
- Check AWS credentials are passed as environment variables

---

Good luck with your grant application! 🚀
