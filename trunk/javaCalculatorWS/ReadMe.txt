1.com.island.stub底下的程式是使用wsdl2java.bat自動產生
2.在Axis2安裝位置的bin資料夾底下會有wsdl2java.bat
3.可使用以下指令自動產生Stub：wsdl2java -uri http://localhost:8080/javaCalculatorWS/services/CalculatorService?wsdl -s -p com.island.stub -o ./stub
  -p可以指定生成的package，-o指定生成的路径
  參考網頁：http://developer.51cto.com/art/200701/37370.htm
  參考網頁：http://hi.baidu.com/aotori/item/c86ebc6c18ac31176995e648
4.把WebService佈署到Server（例如Tomcat），就可以測試
