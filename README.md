bootloader.py module stop -n dve-application -h PL-3
bootloader.py module start -n dve-application -h PL-3

<category name="com.ericsson.jdv.timp">
      <priority value="DEBUG"/>
   </category>

   <category name="com.ericsson.ema.tim">
      <priority value="DEBUG"/>
   </category>

   
CREATE:TIMPOC:userName,eqinson:userId,1000;

echo "# timpoc" >> README.md
git init
git add README.md
git commit -m "first commit"
git remote add origin https://github.com/eqinson2/tim.git
git push -u origin master

