import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Random;
import java.util.ArrayList;

public class CreateData {
    public static void main(String[] args) {
        File data = new File("LinkBookPage.csv");
        Random rand = new Random();

        ArrayList<String> namesList = convertCSVtoArrayList("names.csv");
        ArrayList<String> occupationList = convertCSVtoArrayList("jobs.csv");
        ArrayList<String> educationList = convertCSVtoArrayList("education.csv");



        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            writer.println("ID,Nickname,Occupation,NCode,HighestEdu");

            int namesIndex = 0;
            int numNames = namesList.size();
            int numOccupations = occupationList.size();
            int numEducations = educationList.size();


            for(int i=1; i<=200000; i++){
                int id = i;
                String nickname = namesList.get(namesIndex);
                String occupation = occupationList.get(rand.nextInt(numOccupations));
                int nCode = rand.nextInt(50) + 1;;
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



    public static void createAccessLogs() {
        File accessLogs = new File("accessLogs.csv");
        Random rand = new Random();
        ArrayList<String> typeOfAccessList = convertCSVtoArrayList("typeOfAccess.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(accessLogs))) {
            writer.println("AccessID,ByWho,WhatPage,TypeOfAccess,AccesTime");

            int numAccess = typeOfAccessList.size();

            for(int i=1; i<=100; i++){
                int id = i;
                int byWho = rand.nextInt(200000) + 1;
                int whatPage = rand.nextInt(200000) + 1;
                String typeOfAccess = typeOfAccessList.get(rand.nextInt(numAccess));;
                int AccessTime = rand.nextInt(1000000) + 1;
                if(byWho==whatPage)
                    if (byWho != 200000)
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
}
