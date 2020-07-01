package testartifact;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.example.tutorial.AddressBookProtos.Person;
import com.example.tutorial.AddressBookProtos.Person.PhoneType;

public class ProtobufExample {
	
	public static Person AutoBuildPerson() {
		Person.Builder person = Person.newBuilder();
		person.setId(123467);
		person.setName("First Last");
		person.setEmail("firstlast@domain");
		person.addPhones(Person.PhoneNumber.newBuilder().setNumber("111-222-3333").setType(PhoneType.MOBILE));
		return person.build();
	}
	
	public static Person BuildPerson() {
		Person.Builder person = Person.newBuilder();
		BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
		try {
			System.out.print("Enter person ID: ");
			person.setId(Integer.valueOf(input.readLine()));
			System.out.println("Enter name: ");
			person.setName(input.readLine());
			System.out.println("Enter email address (blank for none): ");
			String email = input.readLine();
			if(email.length() > 0) {
				person.setEmail(email);
			}
			while(true) {
				System.out.println("Enter a phone number (or leave blank to finish): ");
				String number = input.readLine();
				if(number.length() == 0) {
					break;
				}
				Person.PhoneNumber.Builder phoneNumber = Person.PhoneNumber.newBuilder().setNumber(number);
				System.out.println("Is this a mobile, home, or work phone? ");
				String type = input.readLine();
				if(type.equals("mobile")) {
					phoneNumber.setType(Person.PhoneType.MOBILE);
				} else if (type.equals("home")) {
					phoneNumber.setType(Person.PhoneType.HOME);
				} else if (type.equals("work")) {
					phoneNumber.setType(Person.PhoneType.WORK);
				} else {
					System.out.println("Unknown phone type. Using default.");
				}
				person.addPhones(phoneNumber);
			}
			
			return person.build();
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}
	
	private static final String FILE_NAME = "person_serialized";
	
	public static void main(String[] args) {
		Person person = AutoBuildPerson();
		
		System.out.println("Serializing to file: " + FILE_NAME);
		try {
			FileOutputStream output = new FileOutputStream(FILE_NAME);
			person.writeTo(output);
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Deserializing from file: " + FILE_NAME);
		try {
			Person personFromFile = Person.parseFrom(new FileInputStream(FILE_NAME));
			System.out.println("Person ID: " + person.getId());
			System.out.println("Name: " + person.getName());
			System.out.println("Email Address: " + person.getEmail());
			for(Person.PhoneNumber number : person.getPhonesList()) {
				System.out.println("Phone Number: " + number.getNumber() + "     Type: " + number.getType().toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Done");
	}
}
