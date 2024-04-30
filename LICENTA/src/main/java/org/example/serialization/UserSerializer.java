package org.example.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.Serializer;
import org.example.model.User;

public class UserSerializer extends Serializer<User> {
    @Override
    public void write(Kryo kryo, Output output, User user) {
        output.writeString(user.getEmail());
        output.writeString(user.getUsername());
        output.write(user.getAge());
    }

    @Override
    public User read(Kryo kryo, Input input, Class<? extends User> aClass) {
        User user = new User();
        String email = input.readString();
        String name = input.readString();
        int age = input.read();
        user.setAge(age);
        user.setEmail(email);
        user.setUsername(name);

        return  user;
    }
}