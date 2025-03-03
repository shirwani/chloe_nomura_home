# Create Linode VM:
  https://cloud.linode.com/linodes

# Generate new ssh keys using ssh-keygen and add it to the linode being created
# Update the firewall rule to ensure it is accepting HTTP traffic on port 80 and 5001 (assuming that 5001 is the port number for our app)

# ssh into linode like so:
  ssh root@66-228-35-9.ip.linodeusercontent.com
# The followig does not work (if using Akamai employee account)
  ssh root@66.228.35.9

# Run nginx on the linode
# Install nginx
  sudo apt update
  sudo apt install nginx -y
# Start and enable nginx
	sudo systemctl start nginx
  sudo systemctl enable nginx
# Check nginx status
  sudo systemctl status nginx
# Allow firewall access
	sudo ufw allow 'Nginx Full'
  sudo ufw status
# Once nginx is running, in the browser, go to:
  http://66-228-35-9.ip.linodeusercontent.com

# You should see “Welcome to nginx!” message

# Clone a github repo
		mkdir -p /var/www/flask-apps
		cd /var/www/flask-apps
	  git clone https://github.com/shirwani/SampleFlaskAppOnLinode

# Run the flask app
# Install python 3 venv (if not already installed)
cd /var/www/flask-apps/SampleFlaskAppOnLinode
apt install python3.12-venv

# Create python venv in project directory
python3 -m venv venv

# To launch python venv
source venv/bin/activate

# To pip install from requirements.txt (inside python venv)
pip install --upgrade pip
pip install gunicorn
pip install -r requirements.txt

# Run Flask app using gunicorn
#nohup python main.py &
gunicorn -b 0.0.0.0:5001 main:app &

# Configure the app to be accessible from the browser locally (laptop/desktop - not on linode)
# Create the following file on the linode server: 
vi /etc/nginx/sites-available/sites

server {
    listen 80;
    server_name 66-228-35-9.ip.linodeusercontent.com;

    location /puntacana/ {
        proxy_pass http://127.0.0.1:5001/; # Update port number
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        location /puntacana/static {
            alias /var/www/flask-apps/SampleFlaskAppOnLinode/static; # Update location
        }
    }
}

# On the linode server
  sudo ln -s /etc/nginx/sites-available/sites /etc/nginx/sites-enabled
  sudo nginx -t
  sudo service nginx restart
		
# The app is available at http://66-228-35-9.ip.linodeusercontent.com/puntacana/
