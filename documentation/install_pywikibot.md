# Create an account on wiki
- Create your account here: https://exp1.iijlab.net/w/index.php?title=Special:CreateAccount
- And optionally OAuth tokens: Special pages -> OAuth consumer registration -> OAuth consumer registration (make sure you check 'This consumer is for use only by xxx')

# Install pywikibot
- Download the latest version: http://tools.wmflabs.org/pywikibot/core_stable.zip
```
wget http://tools.wmflabs.org/pywikibot/core_stable.zip
```
- Extract the contents of the downloaded zip file in ~/.pywikibot/
```
unzip core_stable.zip -d ~/.pywikibot/
```
- copy pywikibot configuration file from this repo
```
cp conf/pywikibot/families/iyp_family.py ~/.pywikibot/core_stable/pywikibot/families
```
- generate user config file
```
cd ~/.pywikibot/
python core_stable/pwb.py core_stable/generate_user_files.py
# Follow the prompt and select project: iyp
# You can make botpasswords on the wiki page: special pages -> bot passwords
```
- install pywikibot on your system:
```
cd ~/.pywikibot/core_stable/
sudo python3 setup.py install
```

# Tweaks (~/.pywikibot/user-config.py)
- no throttle: add the following in user-config.py:
```
# Slow down the robot such that it never makes a second page edit within
# 'put_throttle' seconds.
put_throttle = 0
```
- add your OAuth credentials
```
# Replace the four fields with the value obtained on the wiki OAuth page
authenticate['*.iijlab.net'] = ('consumer_token', 'consumer_secret', 'access_token', 'access_secret')
```
