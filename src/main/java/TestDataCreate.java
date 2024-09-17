import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Random;
import java.util.ArrayList;

public class TestDataCreate {
    public static void createLinkBookPage() {
        File data = new File("TestLinkBookPage.csv");
        Random rand = new Random();

        ArrayList<String> namesList = convertCSVtoArrayList("names.csv");
        ArrayList<String> occupationList = convertCSVtoArrayList("jobs.csv");
        ArrayList<String> educationList = convertCSVtoArrayList("education.csv");


        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            //writer.println("ID,Nickname,Occupation,NCode,HighestEdu");

            int namesIndex = 0;
            int numNames = namesList.size();
            int numOccupations = occupationList.size();
            int numEducations = educationList.size();


            for(int i=1; i<=100; i++){
                int id = i;
                String nickname = namesList.get(namesIndex);
                String occupation = occupationList.get(rand.nextInt(numOccupations));
                int nCode = rand.nextInt(50) + 1;
                String highestEdu = educationList.get(rand.nextInt(numEducations));
                writer.println(id + "," + nickname + "," + occupation + "," + nCode + "," + highestEdu);
                if(namesIndex==numNames-1)
                    namesIndex=0;
                else
                    namesIndex++;
            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");
            e.printStackTrace();
        }
    }

    public static void createAssociates(){
        File data = new File("TestAssociates.csv");
        Random rand = new Random(35434523);

        ArrayList<String[]> namesList = convertCSVtoArrayArrayList("TestLinkBookPage.csv");
        ArrayList<String> descList = convertCSVtoArrayList("description.csv");
        ArrayList<ArrayList<Integer>> listOfAssociations = new ArrayList<>();

        for(int i=0; i<100; i++){
            listOfAssociations.add(new ArrayList<>());
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            //writer.println("ColRel,ID1,ID2,DateOfRelation,Desc");

            int i=1;
            for(int j=0; j<namesList.size(); j++){
                for(int k=0; k<5; k++){
                    int ColRel = i;
                    int ID1 = Integer.parseInt(namesList.get(j)[0]);
                    int ID2 = rand.nextInt(namesList.size())+1;
                    while(true){
                        if(ID2==ID1)
                            ID2 = rand.nextInt(namesList.size())+1;
                        else{
                            //need ID1-1 to get index in arraylist
                            //this is checking for duplicates relations of 1 to 2
                            ArrayList<Integer> ID1Associations = listOfAssociations.get(ID1-1);
                            for (Integer id1Association : ID1Associations) {
                                if (id1Association == ID2)
                                    ID2 = rand.nextInt(namesList.size())+1;
                            }

                            //this is checking for duplicate associations of 2 to 1 to make sure its symmetric
                            ArrayList<Integer> ID2Associations = listOfAssociations.get(ID2-1);
                            for (Integer id2Association : ID2Associations) {
                                if (id2Association == ID1)
                                    ID2 = rand.nextInt(namesList.size())+1;
                            }
                            break;
                        }
                    }
                    listOfAssociations.get(ID1-1).add(ID2);
                    int DateOfRelation = rand.nextInt(50) + 1;
                    String Desc = descList.get(rand.nextInt(descList.size()));

                    //writing it to csv and add to arraylist
                    //String[] association = {Integer.toString(ColRel), Integer.toString(ID1), Integer.toString(ID2), Integer.toString(DateOfRelation), Desc};
                    writer.println(ColRel + "," + ID1 + "," + ID2 + "," + DateOfRelation + "," + Desc);
                    i++;
                }


            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");
            e.printStackTrace();
        }
    }

    public static void createAccessLogs() {
        File accessLogs = new File("TestaccessLogs.csv");
        Random rand = new Random();
        ArrayList<String> typeOfAccessList = convertCSVtoArrayList("typeOfAccess.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(accessLogs))) {
            //writer.println("AccessID,ByWho,WhatPage,TypeOfAccess,AccessTime");

            int numAccess = typeOfAccessList.size();

            for(int i=1; i<=200; i++){
                int id = i;
                int byWho = rand.nextInt(100) + 1;
                int whatPage = rand.nextInt(100) + 1;
                String typeOfAccess = typeOfAccessList.get(rand.nextInt(numAccess));
                int AccessTime = rand.nextInt(100) + 1;
                if(byWho==whatPage)
                    if (byWho != 100)
                        whatPage = byWho + 1;
                    else
                        whatPage = byWho - 1;
                writer.println(id + "," + byWho + "," + whatPage + "," + typeOfAccess + "," + AccessTime);

            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");
            e.printStackTrace();
        }
    }

    public static ArrayList<String[]> convertCSVtoArrayArrayList(String filePath){
        String line = "";
        String delimiter = ",";
        ArrayList<String[]> words = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Iterate through the file line by line
            while ((line = br.readLine()) != null) {
                String[] word = line.split(delimiter); //split at comma
                words.add(word);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return words;
    }

    public static ArrayList<String> convertCSVtoArrayList(String filePath){
        String line = "";
        String delimiter = ",";
        ArrayList<String> words = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Iterate through the file line by line
            while ((line = br.readLine()) != null) {
                String[] word = line.split(delimiter); //split at comma
                words.addAll(Arrays.asList(word));
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return words;
    }



    public static void main(String[] args) {
        //createLinkBookPage();
        //createAssociates();
        createAccessLogs();
    }
}
