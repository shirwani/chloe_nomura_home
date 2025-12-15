scp ./chloe.db                root@66-228-35-9.ip.linodeusercontent.com:/var/www/flask-apps/chloe_nomura_home/.
scp ./static/images/*         root@66-228-35-9.ip.linodeusercontent.com:/var/www/flask-apps/chloe_nomura_home/static/images/.
scp ./secrets/*               root@66-228-35-9.ip.linodeusercontent.com:/var/www/flask-apps/chloe_nomura_home/secrets/.


server {
    listen 80;
    server_name 66-228-35-9.ip.linodeusercontent.com zakishirwani.com www.zakishirwani.com;

    location /puntacana/ {
        proxy_pass http://127.0.0.1:5001/; # Update port number
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        location /puntacana/static {
            alias /var/www/flask-apps/SampleFlaskAppOnLinode/static; # Update location
        }
    }

    location /ai/digits/ {
        proxy_pass http://127.0.0.1:5002/; # Update port number
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        location /ai/digits/static {
            alias /var/www/flask-apps/HandWrittenDigitRecognition/static; # Update location
        }
    }

    location /ai/animals/ {
        proxy_pass http://127.0.0.1:5003/; # Update port number
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        location /ai/animals/static {
            alias /var/www/flask-apps/DeepAnimalClassifier/static; # Update location
        }
    }

    location /ai/cats/ {
        proxy_pass http://127.0.0.1:5004/; # Update port number
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        location /ai/cats/static {
            alias /var/www/flask-apps/CatIdentifier/static; # Update location
        }
    }

    location /sp500/ {
        proxy_pass http://127.0.0.1:5006/; # Update port number
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        location /sp500/static {
            alias /var/www/flask-apps/Sp500_doublers/static; # Update location
        }
    }

    location /goai/ {
        # Remove /goai prefix before passing to Flask
        rewrite ^/goai/(.*)$ /$1 break;

        # Proxy to your Flask app (UPDATED PORT)
        proxy_pass http://127.0.0.1:5005;  # Changed from 5000 to 5005
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Handle static files
        location /goai/static/ {
            alias /var/www/flask-apps/goai/static/;  # Update this path
            expires 1y;
            add_header Cache-Control "public, immutable";
        }
    }

    location /chloe_nomura_home/ {
        rewrite ^/chloe_nomura_home/(.*)$ /$1 break;
        proxy_pass http://127.0.0.1:5007;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Rewrite any app redirect like "Location: /..." back under /chloe_nomura_home
        proxy_redirect ~^(/.*)$ /chloe_nomura_home$1;

        location /chloe_nomura_home/static/ {
            alias /var/www/flask-apps/chloe_nomura_home/static/;
        }
    }

    # ALSO forward the app's root URLs (like /login, /inventory, etc.)
    location ~ ^/(login|signup|inventory|contact|cart|checkout|api|static|forgot-password|reset-password) {
        proxy_pass http://127.0.0.1:5007;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Same redirect rewriting here
        proxy_redirect ~^(/.*)$ /chloe_nomura_home$1;
    }


    
}
